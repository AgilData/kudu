// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/schema.h"

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "kudu/client/schema-internal.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"

MAKE_ENUM_LIMITS(kudu::client::KuduColumnStorageAttributes::EncodingType,
                 kudu::client::KuduColumnStorageAttributes::AUTO_ENCODING,
                 kudu::client::KuduColumnStorageAttributes::RLE);

MAKE_ENUM_LIMITS(kudu::client::KuduColumnStorageAttributes::CompressionType,
                 kudu::client::KuduColumnStorageAttributes::DEFAULT_COMPRESSION,
                 kudu::client::KuduColumnStorageAttributes::ZLIB);

MAKE_ENUM_LIMITS(kudu::client::KuduColumnSchema::DataType,
                 kudu::client::KuduColumnSchema::INT8,
                 kudu::client::KuduColumnSchema::BOOL);

namespace kudu {

namespace client {

using std::vector;

kudu::EncodingType ToInternalEncodingType(KuduColumnStorageAttributes::EncodingType type) {
  switch (type) {
    case KuduColumnStorageAttributes::AUTO_ENCODING: return kudu::AUTO_ENCODING;
    case KuduColumnStorageAttributes::PLAIN_ENCODING: return kudu::PLAIN_ENCODING;
    case KuduColumnStorageAttributes::PREFIX_ENCODING: return kudu::PREFIX_ENCODING;
    case KuduColumnStorageAttributes::DICT_ENCODING: return kudu::DICT_ENCODING;
    case KuduColumnStorageAttributes::GROUP_VARINT: return kudu::GROUP_VARINT;
    case KuduColumnStorageAttributes::RLE: return kudu::RLE;
    case KuduColumnStorageAttributes::BIT_SHUFFLE: return kudu::BIT_SHUFFLE;
    default: LOG(FATAL) << "Unexpected encoding type: " << type;
  }
}

KuduColumnStorageAttributes::EncodingType FromInternalEncodingType(kudu::EncodingType type) {
  switch (type) {
    case kudu::AUTO_ENCODING: return KuduColumnStorageAttributes::AUTO_ENCODING;
    case kudu::PLAIN_ENCODING: return KuduColumnStorageAttributes::PLAIN_ENCODING;
    case kudu::PREFIX_ENCODING: return KuduColumnStorageAttributes::PREFIX_ENCODING;
    case kudu::DICT_ENCODING: return KuduColumnStorageAttributes::DICT_ENCODING;
    case kudu::GROUP_VARINT: return KuduColumnStorageAttributes::GROUP_VARINT;
    case kudu::RLE: return KuduColumnStorageAttributes::RLE;
    case kudu::BIT_SHUFFLE: return KuduColumnStorageAttributes::BIT_SHUFFLE;
    default: LOG(FATAL) << "Unexpected internal encoding type: " << type;
  }
}

kudu::CompressionType ToInternalCompressionType(KuduColumnStorageAttributes::CompressionType type) {
  switch (type) {
    case KuduColumnStorageAttributes::DEFAULT_COMPRESSION: return kudu::DEFAULT_COMPRESSION;
    case KuduColumnStorageAttributes::NO_COMPRESSION: return kudu::NO_COMPRESSION;
    case KuduColumnStorageAttributes::SNAPPY: return kudu::SNAPPY;
    case KuduColumnStorageAttributes::LZ4: return kudu::LZ4;
    case KuduColumnStorageAttributes::ZLIB: return kudu::ZLIB;
    default: LOG(FATAL) << "Unexpected compression type" << type;
  }
}

KuduColumnStorageAttributes::CompressionType FromInternalCompressionType(
    kudu::CompressionType type) {
  switch (type) {
    case kudu::DEFAULT_COMPRESSION: return KuduColumnStorageAttributes::DEFAULT_COMPRESSION;
    case kudu::NO_COMPRESSION: return KuduColumnStorageAttributes::NO_COMPRESSION;
    case kudu::SNAPPY: return KuduColumnStorageAttributes::SNAPPY;
    case kudu::LZ4: return KuduColumnStorageAttributes::LZ4;
    case kudu::ZLIB: return KuduColumnStorageAttributes::ZLIB;
    default: LOG(FATAL) << "Unexpected internal compression type: " << type;
  }
}

kudu::DataType ToInternalDataType(KuduColumnSchema::DataType type) {
  switch (type) {
    case KuduColumnSchema::INT8: return kudu::INT8;
    case KuduColumnSchema::INT16: return kudu::INT16;
    case KuduColumnSchema::INT32: return kudu::INT32;
    case KuduColumnSchema::INT64: return kudu::INT64;
    case KuduColumnSchema::FLOAT: return kudu::FLOAT;
    case KuduColumnSchema::DOUBLE: return kudu::DOUBLE;
    case KuduColumnSchema::STRING: return kudu::STRING;
    case KuduColumnSchema::BOOL: return kudu::BOOL;
    default: LOG(FATAL) << "Unexpected data type: " << type;
  }
}

KuduColumnSchema::DataType FromInternalDataType(kudu::DataType type) {
  switch (type) {
    case kudu::INT8: return KuduColumnSchema::INT8;
    case kudu::INT16: return KuduColumnSchema::INT16;
    case kudu::INT32: return KuduColumnSchema::INT32;
    case kudu::INT64: return KuduColumnSchema::INT64;
    case kudu::FLOAT: return KuduColumnSchema::FLOAT;
    case kudu::DOUBLE: return KuduColumnSchema::DOUBLE;
    case kudu::STRING: return KuduColumnSchema::STRING;
    case kudu::BOOL: return KuduColumnSchema::BOOL;
    default: LOG(FATAL) << "Unexpected internal data type: " << type;
  }
}

std::string KuduColumnSchema::DataTypeToString(DataType type) {
  return DataType_Name(ToInternalDataType(type));
}

KuduColumnSchema::KuduColumnSchema(const std::string &name,
                                   DataType type,
                                   bool is_nullable,
                                   const void* default_value,
                                   KuduColumnStorageAttributes attributes) {
  ColumnStorageAttributes attr_private(ToInternalEncodingType(attributes.encoding()),
                                       ToInternalCompressionType(attributes.compression()));
  col_ = new ColumnSchema(name, ToInternalDataType(type), is_nullable,
                          default_value, default_value, attr_private);
}

KuduColumnSchema::KuduColumnSchema(const KuduColumnSchema& other)
  : col_(NULL) {
  CopyFrom(other);
}

KuduColumnSchema::~KuduColumnSchema() {
  delete col_;
}

KuduColumnSchema& KuduColumnSchema::operator=(const KuduColumnSchema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduColumnSchema::CopyFrom(const KuduColumnSchema& other) {
  delete col_;
  col_ = new ColumnSchema(*other.col_);
}

bool KuduColumnSchema::Equals(const KuduColumnSchema& other) const {
  return this == &other || col_->Equals(*other.col_, true);
}

const std::string& KuduColumnSchema::name() const {
  return col_->name();
}

bool KuduColumnSchema::is_nullable() const {
  return col_->is_nullable();
}

KuduColumnSchema::DataType KuduColumnSchema::type() const {
  return FromInternalDataType(col_->type_info()->type());
}


KuduSchema::KuduSchema()
  : schema_(NULL) {
}

KuduSchema::KuduSchema(const vector<KuduColumnSchema>& columns, int key_columns)
  : schema_(NULL) {
  Reset(columns, key_columns);
}

KuduSchema::KuduSchema(const KuduSchema& other)
  : schema_(NULL) {
  CopyFrom(other);
}

KuduSchema::~KuduSchema() {
  delete schema_;
}

KuduSchema& KuduSchema::operator=(const KuduSchema& other) {
  if (&other != this) {
    CopyFrom(other);
  }
  return *this;
}

void KuduSchema::CopyFrom(const KuduSchema& other) {
  delete schema_;
  schema_ = new Schema(*other.schema_);
}

void KuduSchema::Reset(const vector<KuduColumnSchema>& columns, int key_columns) {
  vector<ColumnSchema> cols_private;
  BOOST_FOREACH(const KuduColumnSchema& col, columns) {
    cols_private.push_back(*col.col_);
  }
  delete schema_;
  schema_ = new Schema(cols_private, key_columns);
}

bool KuduSchema::Equals(const KuduSchema& other) const {
  return this == &other ||
      (schema_ && other.schema_ && schema_->Equals(*other.schema_));
}

KuduColumnSchema KuduSchema::Column(size_t idx) const {
  ColumnSchema col(schema_->column(idx));
  KuduColumnStorageAttributes attrs(FromInternalEncodingType(col.attributes().encoding()),
                                    FromInternalCompressionType(col.attributes().compression()));
  return KuduColumnSchema(col.name(), FromInternalDataType(col.type_info()->type()),
                          col.is_nullable(), col.read_default_value(),
                          attrs);
}

KuduSchema KuduSchema::CreateKeyProjection() const {
  KuduSchema projection;
  projection.schema_ = new Schema(schema_->CreateKeyProjection());
  return projection;
}

KuduPartialRow* KuduSchema::NewRow() const {
  return new KuduPartialRow(schema_);
}

size_t KuduSchema::num_columns() const {
  return schema_->num_columns();
}

size_t KuduSchema::num_key_columns() const {
  return schema_->num_key_columns();
}

} // namespace client
} // namespace kudu
