// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include <algorithm>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/bshuf_block.h"
#include "kudu/cfile/string_dict_block.h"
#include "kudu/common/columnblock.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/group_varint-inl.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/memory/arena.h"

namespace kudu {
namespace cfile {

StringDictBlockBuilder::StringDictBlockBuilder(const WriterOptions* options)
  : options_(options),
    dict_block_(options_),
    dictionary_strings_arena_(1024, 32*1024*1024),
    mode_(kCodeWordMode) {
  data_builder_.reset(new BShufBlockBuilder<UINT16>(options_));
  Reset();
}

void StringDictBlockBuilder::Reset() {
  buffer_.clear();
  buffer_.resize(kMaxHeaderSize);
  buffer_.reserve(options_->block_size);

  if ((mode_ == kCodeWordMode) && dict_block_.IsBlockFull(options_->block_size)) {
    mode_ = kPlainStringMode;
    data_builder_.reset(new StringPlainBlockBuilder(options_));
  } else {
    data_builder_->Reset();
  }

  finished_ = false;
}

Slice StringDictBlockBuilder::Finish(rowid_t ordinal_pos) {
  finished_ = true;

  InlineEncodeFixed32(&buffer_[0], mode_);

  // TODO: if we could modify the the Finish() API a little bit, we can
  // avoid an extra memory copy (buffer_.append(..))
  Slice data_slice = data_builder_->Finish(ordinal_pos);
  buffer_.append(data_slice.data(), data_slice.size());

  return Slice(buffer_);
}

// The current block is considered full when the the size of data block
// exceeds limit or when the size of dictionary block exceeds options_->block_size.
//
// If it is the latter case, all the subsequent data blocks will switch to
// StringPlainBlock automatically.
bool StringDictBlockBuilder::IsBlockFull(size_t limit) const {
  if (data_builder_->IsBlockFull(options_->block_size)) return true;
  if (dict_block_.IsBlockFull(options_->block_size) && (mode_ == kCodeWordMode)) return true;
  return false;
}

int StringDictBlockBuilder::AddCodeWords(const uint8_t* vals, size_t count) {
  DCHECK(!finished_);
  DCHECK_GT(count, 0);
  size_t i;

  if (data_builder_->Count() == 0) {
    const Slice* first = reinterpret_cast<const Slice*>(vals);
    first_key_.assign_copy(first->data(), first->size());
  }

  for (i = 0; i < count; i++) {
    const Slice* src = reinterpret_cast<const Slice*>(vals);
    const char* c_str = reinterpret_cast<const char*>(src->data());
    StringPiece current_item(c_str, src->size());
    uint16_t codeword;

    if (!FindCopy(dictionary_, current_item, &codeword)) {
      // The dictionary block is full
      if (dict_block_.Add(vals, 1) == 0) {
        break;
      }
      const uint8_t* s_ptr = dictionary_strings_arena_.AddSlice(*src);
      if (s_ptr == NULL) {
        // Arena does not have enough space for string content
        // Ideally, it should not happen.
        LOG(ERROR) << "Arena of Dictionary Encoder does not have enough memory for strings";
        break;
      }
      const char* s_content = reinterpret_cast<const char*>(s_ptr);
      codeword = dict_block_.Count() - 1;
      InsertOrDie(&dictionary_, StringPiece(s_content, src->size()), codeword);
    }
    // The data block is full
    if (data_builder_->Add(reinterpret_cast<const uint8_t*>(&codeword), 1) == 0) {
      break;
    }
    vals += sizeof(Slice);
  }
  return i;
}

int StringDictBlockBuilder::Add(const uint8_t* vals, size_t count) {
  if (mode_ == kCodeWordMode) {
    return AddCodeWords(vals, count);
  } else {
    DCHECK_EQ(mode_, kPlainStringMode);
    return data_builder_->Add(vals, count);
  }
}

Status StringDictBlockBuilder::AppendExtraInfo(CFileWriter* c_writer, CFileFooterPB* footer) {
  Slice dict_slice = dict_block_.Finish(0);

  std::vector<Slice> dict_v;
  dict_v.push_back(dict_slice);

  BlockPointer ptr;
  Status s = c_writer->AppendDictBlock(dict_v, &ptr, "Append dictionary block");
  if (!s.ok()) {
    LOG(WARNING) << "Unable to append block to file: " << s.ToString();
    return s;
  }
  ptr.CopyToPB(footer->mutable_dict_block_ptr());
  return Status::OK();
}

uint64_t StringDictBlockBuilder::Count() const {
  return data_builder_->Count();
}

Status StringDictBlockBuilder::GetFirstKey(void* key_void) const {
  if (mode_ == kCodeWordMode) {
    CHECK(finished_);
    Slice* slice = reinterpret_cast<Slice*>(key_void);
    *slice = Slice(first_key_);
    return Status::OK();
  } else {
    DCHECK_EQ(mode_, kPlainStringMode);
    return data_builder_->GetFirstKey(key_void);
  }
}

////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////

StringDictBlockDecoder::StringDictBlockDecoder(const Slice& slice, CFileIterator* iter)
  : data_(slice),
    parsed_(false) {
  dict_decoder_ = iter->GetDictDecoder();
}

Status StringDictBlockDecoder::ParseHeader() {
  CHECK(!parsed_);

  if (data_.size() < kMinHeaderSize) {
    return Status::Corruption(
      strings::Substitute("not enough bytes for header: dictionary block header "
        "size ($0) less than minimum possible header length ($1)",
        data_.size(), kMinHeaderSize));
  }

  bool valid = tight_enum_test_cast<DictEncodingMode>(DecodeFixed32(&data_[0]), &mode_);
  if (PREDICT_FALSE(!valid)) {
    return Status::Corruption("header Mode information corrupted");
  }
  Slice content(data_.data() + 4, data_.size() - 4);

  if (mode_ == kCodeWordMode) {
    data_decoder_.reset(new BShufBlockDecoder<UINT16>(content));
  } else {
    if (mode_ != kPlainStringMode) {
      return Status::Corruption("Unrecognized Dictionary encoded data block header");
    }
    data_decoder_.reset(new StringPlainBlockDecoder(content));
  }

  RETURN_NOT_OK(data_decoder_->ParseHeader());
  parsed_ = true;
  return Status::OK();
}

void StringDictBlockDecoder::SeekToPositionInBlock(uint pos) {
  data_decoder_->SeekToPositionInBlock(pos);
}

Status StringDictBlockDecoder::SeekAtOrAfterValue(const void* value_void, bool* exact) {
  if (mode_ == kCodeWordMode) {
    DCHECK(value_void != NULL);
    Status s = dict_decoder_->SeekAtOrAfterValue(value_void, exact);
    if (!s.ok()) {
      // This case means the value_void is larger that the largest key
      // in the dictionary block. Therefore, it is impossible to be in
      // the current data block, and we adjust the index to be the end
      // of the block
      data_decoder_->SeekToPositionInBlock(data_decoder_->Count() - 1);
      return s;
    }

    size_t index = dict_decoder_->GetCurrentIndex();
    bool tmp;
    return data_decoder_->SeekAtOrAfterValue(&index, &tmp);
  } else {
    DCHECK_EQ(mode_, kPlainStringMode);
    return data_decoder_->SeekAtOrAfterValue(value_void, exact);
  }
}

Status StringDictBlockDecoder::CopyNextDecodeStrings(size_t* n, ColumnDataView* dst) {
  DCHECK(parsed_);
  DCHECK_EQ(dst->type_info()->type(), STRING);
  DCHECK_LE(*n, dst->nrows());
  DCHECK_EQ(dst->stride(), sizeof(Slice));

  Arena* out_arena = dst->arena();
  Slice* out = reinterpret_cast<Slice*>(dst->data());

  codeword_buf_.resize((*n)*sizeof(uint16_t));

  // Copy the codewords into a temporary buffer first.
  // And then Copy the strings corresponding to the codewords to the destination buffer.
  BShufBlockDecoder<UINT16>* d_bptr = down_cast<BShufBlockDecoder<UINT16>*>(data_decoder_.get());
  RETURN_NOT_OK(d_bptr->CopyNextValuesToArray(n, codeword_buf_.data()));

  for (int i = 0; i < *n; i++) {
    uint16_t codeword = *reinterpret_cast<uint16_t*>(&codeword_buf_[i*sizeof(uint16_t)]);
    Slice elem = dict_decoder_->string_at_index(codeword);
    CHECK(PREDICT_TRUE(out_arena->RelocateSlice(elem, out)));
    out++;
  }
  return Status::OK();
}

Status StringDictBlockDecoder::CopyNextValues(size_t* n, ColumnDataView* dst) {
  if (mode_ == kCodeWordMode) {
    return CopyNextDecodeStrings(n, dst);
  } else {
    DCHECK_EQ(mode_, kPlainStringMode);
    return data_decoder_->CopyNextValues(n, dst);
  }
}

} // namespace cfile
} // namespace kudu
