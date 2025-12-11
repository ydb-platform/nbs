#include "parser.h"

#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

TFreeList ParseFreeSpaceLine(const TString& line)
{
    TStringInput stream(line);

    TFreeList fl;

    stream >> fl.GroupNo >> fl.Offset >> fl.Count;

    return fl;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSuperBlock ParseSuperBlock(IInputStream& stream)
{
    TSuperBlock sb;

    TString line;
    while (stream.ReadLine(line)) {
        TStringBuf name, value;
        TStringBuf(line).Split(" = ", name, value);

        if (name == "blocksize") {
            sb.BlockSize = FromString<ui64>(value);
        } else if (name == "agcount") {
            sb.GroupCount = FromString<ui64>(value);
        } else if (name == "agblocks") {
            sb.BlocksPerGroup = FromString<ui64>(value);
        } else if (name == "sectsize") {
            sb.SectorSize = FromString<ui64>(value);
        }
    }

    return sb;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TFreeList> ParseFreeSpace(IInputStream& stream)
{
    TVector<TFreeList> freesp;

    TString line;

    // header
    Y_ENSURE(stream.ReadLine(line), "can't read header");
    Y_ENSURE(Strip(line).StartsWith("agno"));

    while (stream.ReadLine(line)) {
        if (Strip(line).StartsWith("from")) {
            // footer
            break;
        }

        freesp.push_back(ParseFreeSpaceLine(line));
    }

    return freesp;
}
