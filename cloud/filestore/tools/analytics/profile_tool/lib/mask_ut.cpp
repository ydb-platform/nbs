#include "mask.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMaskSensitiveData)
{
    Y_UNIT_TEST(ShouldMaskBrokenFilenames)
    {
        {
            TMaskSensitiveData mask(
                TMaskSensitiveData::EMode::NodeId,
                "seed",
                5);

            UNIT_ASSERT_VALUES_EQUAL("nodeid-2", mask.Transform("1", 2));
            UNIT_ASSERT_VALUES_EQUAL(
                "nodeid-3.cpp",
                mask.Transform("1.cpp", 3));
            UNIT_ASSERT_VALUES_EQUAL("nodeid-4", mask.Transform(".cpp", 4));
            UNIT_ASSERT_VALUES_EQUAL(
                "nodeid-5",
                mask.Transform(
                    "\x02\x00\x00\x00\x01\x00\6\x00\xFF\xFF\xFF\xFF\x04\x00",
                    5));
            UNIT_ASSERT_VALUES_EQUAL("nodeid-6", mask.Transform("1.", 6));
            UNIT_ASSERT_VALUES_EQUAL("nodeid-7", mask.Transform("", 7));
            UNIT_ASSERT_VALUES_EQUAL("nodeid-8", mask.Transform("\x00", 8));
            UNIT_ASSERT_VALUES_EQUAL("nodeid-9", mask.Transform("\x00.cpp", 9));
            UNIT_ASSERT_VALUES_EQUAL("nodeid-10", mask.Transform("1.\x00", 10));
            UNIT_ASSERT_VALUES_EQUAL(
                "nodeid-11.cpp",
                mask.Transform("1.2.3.cpp", 11));
            UNIT_ASSERT_VALUES_EQUAL(
                "nodeid-12",
                mask.Transform("1.longexthere", 12));

            {
                NProto::TProfileLogRequestInfo request;
                request.MutableNodeInfo()->SetNodeId(1);
                request.MutableNodeInfo()->SetNodeName("nodename");
                request.MutableNodeInfo()->SetNewNodeName("newnodename.cpp");
                mask.MaskRequest(request);
                UNIT_ASSERT_VALUES_EQUAL(
                    "nodeid-1",
                    request.GetNodeInfo().GetNodeName());
                UNIT_ASSERT_VALUES_EQUAL(
                    "nodeid-1.cpp",
                    request.GetNodeInfo().GetNewNodeName());
            }

            {
                // Empty filename check
                NProto::TProfileLogRequestInfo request;
                request.MutableNodeInfo()->SetNodeId(2);
                mask.MaskRequest(request);
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNodeName());
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNewNodeName());
            }
        }

        {
            TMaskSensitiveData mask(TMaskSensitiveData::EMode::Hash, "seed", 5);

            UNIT_ASSERT_VALUES_EQUAL(
                "9289941f8a4200fd6c05f11fab6bc867",
                mask.Transform("1", 2));
            UNIT_ASSERT_VALUES_EQUAL(
                "1cf71d01de6819bc4af63094918508d5.cpp",
                mask.Transform("1.cpp", 3));
            UNIT_ASSERT_VALUES_EQUAL(
                "fe530ef67c2101807d253963c5dac9b2",
                mask.Transform(".cpp", 4));
            UNIT_ASSERT_VALUES_EQUAL(
                "5c533bc9b3ce62c0b1e91739e26e94ee",
                mask.Transform(
                    "\x02\x00\x00\x00\x01\x00\6\x00\xFF\xFF\xFF\xFF\x04\x00",
                    5));
            UNIT_ASSERT_VALUES_EQUAL(
                "fe4c0f30aa359c41d9f9a5f69c8c4192",
                mask.Transform("", 6));

            {
                NProto::TProfileLogRequestInfo request;
                request.MutableNodeInfo()->SetNodeId(1);
                request.MutableNodeInfo()->SetNodeName("nodename");
                request.MutableNodeInfo()->SetNewNodeName("newnodename.cpp");
                mask.MaskRequest(request);
                UNIT_ASSERT_VALUES_EQUAL(
                    "17334584d7bdefeb7a2e7afa815b78b9",
                    request.GetNodeInfo().GetNodeName());
                UNIT_ASSERT_VALUES_EQUAL(
                    "7baafa832c29db2dd9685fd11f7b182d.cpp",
                    request.GetNodeInfo().GetNewNodeName());
            }
            {
                // Empty filename check
                NProto::TProfileLogRequestInfo request;
                request.MutableNodeInfo()->SetNodeId(2);
                mask.MaskRequest(request);
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNodeName());
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNewNodeName());
            }
        }

        {
            TMaskSensitiveData mask(
                TMaskSensitiveData::EMode::Empty,
                "seed",
                5);

            UNIT_ASSERT_VALUES_EQUAL("", mask.Transform("1", 2));
            UNIT_ASSERT_VALUES_EQUAL(".cpp", mask.Transform("1.cpp", 3));
            UNIT_ASSERT_VALUES_EQUAL("", mask.Transform(".cpp", 4));
            UNIT_ASSERT_VALUES_EQUAL(
                "",
                mask.Transform(
                    "\x02\x00\x00\x00\x01\x00\6\x00\xFF\xFF\xFF\xFF\x04\x00",

                    5));
            UNIT_ASSERT_VALUES_EQUAL("", mask.Transform("", 6));

            {
                NProto::TProfileLogRequestInfo request;
                request.MutableNodeInfo()->SetNodeId(1);
                request.MutableNodeInfo()->SetNodeName("nodename");
                request.MutableNodeInfo()->SetNewNodeName("newnodename.h");
                mask.MaskRequest(request);
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNodeName());
                UNIT_ASSERT_VALUES_EQUAL(
                    ".h",
                    request.GetNodeInfo().GetNewNodeName());
            }
            {
                // Empty filename check
                NProto::TProfileLogRequestInfo request;
                request.MutableNodeInfo()->SetNodeId(2);
                mask.MaskRequest(request);
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNodeName());
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request.GetNodeInfo().GetNewNodeName());
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
