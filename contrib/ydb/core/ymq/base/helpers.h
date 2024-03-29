#pragma once
#include <contrib/ydb/core/ymq/proto/records.pb.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NSQS {

// Validation of deduplication id, group id, receive request attempt id
bool IsAlphaNumAndPunctuation(TStringBuf str);

// https://docs.aws.amazon.com/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-attributes.html
bool ValidateMessageAttributeName(TStringBuf str, bool& hasYandexPrefix, bool allowYandexPrefix = false);

bool ValidateQueueNameOrUserName(TStringBuf name);

// Validation function for message body or string message attributes
bool ValidateMessageBody(TStringBuf body, TString& errorDescription);

TString EncodeReceiptHandle(const TReceipt& receipt);
TReceipt DecodeReceiptHandle(const TString& receipt);

} // namespace NKikimr::NSQS
