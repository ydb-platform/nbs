#include "protocol.h"

#include <util/system/yassert.h>

#include <cstring>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

TOptionWriter::TOptionWriter(void* buffer, size_t capacity)
    : Begin(static_cast<char*>(buffer))
    , End(Begin + capacity)
    , Ptr(Begin)
{}

size_t TOptionWriter::Size() const
{
    return Ptr - Begin;
}

void TOptionWriter::WriteData(ui8 flag, TStringBuf data)
{
    Y_ABORT_UNLESS(data.size() <= Max<ui8>());
    const ui8 size = static_cast<ui8>(data.size());

    WriteFlag(flag);
    Write(&size, sizeof(size));
    Write(data.data(), data.size());
}

void TOptionWriter::WriteValueImpl(ui8 flag, const void* value, size_t size)
{
    WriteFlag(flag);
    Write(value, size);
}

void TOptionWriter::WriteFlag(ui8 flag)
{
    Y_ABORT_UNLESS(flag > LastFlag);
    LastFlag = flag;
    Write(&flag, sizeof(flag));
}

void TOptionWriter::Write(const void* data, size_t size)
{
    Y_ABORT_UNLESS(size <= static_cast<size_t>(End - Ptr));
    if (size) {
        memcpy(Ptr, data, size);
        Ptr += size;
    }
}

////////////////////////////////////////////////////////////////////////////////

TOptionReader::TOptionReader(const void* options, size_t size)
    : Ptr(static_cast<const char*>(options))
    , End(Ptr + size)
{}

bool TOptionReader::ReadData(ui8 flag, TStringBuf* data)
{
    if (!IsNext(flag)) {
        return true;
    }
    if (Left() < sizeof(flag) + sizeof(ui8)) {
        return false;
    }

    const size_t size = static_cast<ui8>(Ptr[sizeof(flag)]);
    if (Left() < sizeof(flag) + sizeof(ui8) + size) {
        return false;
    }

    *data = TStringBuf(Ptr + sizeof(flag) + sizeof(ui8), size);
    Ptr += sizeof(flag) + sizeof(ui8) + size;
    return true;
}

bool TOptionReader::ReadValueImpl(ui8 flag, void* value, size_t size)
{
    if (!IsNext(flag)) {
        return true;
    }
    if (Left() < sizeof(flag) + size) {
        return false;
    }

    memcpy(value, Ptr + sizeof(flag), size);
    Ptr += sizeof(flag) + size;
    return true;
}

bool TOptionReader::IsNext(ui8 flag)
{
    Y_ABORT_UNLESS(flag > LastFlag);
    LastFlag = flag;
    return Ptr < End && static_cast<ui8>(*Ptr) == flag;
}

size_t TOptionReader::Left() const
{
    return End - Ptr;
}

}   // namespace NCloud::NStorage::NRdma
