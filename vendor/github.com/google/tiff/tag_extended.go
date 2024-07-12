// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

// TODO: Pass in the slice of valid FieldType for each tag.

var ExtendedTags = NewTagSet("Extended", 1, 64999)

func init() {
    ExtendedTags.Register(NewTag(269, "DocumentName", nil))
    ExtendedTags.Register(NewTag(285, "PageName", nil))
    ExtendedTags.Register(NewTag(286, "XPosition", nil))
    ExtendedTags.Register(NewTag(287, "YPosition", nil))
    ExtendedTags.Register(NewTag(292, "T4Options", nil))
    ExtendedTags.Register(NewTag(293, "T6Options", nil))
    ExtendedTags.Register(NewTag(297, "PageNumber", nil))
    ExtendedTags.Register(NewTag(301, "TransferFunction", nil))
    ExtendedTags.Register(NewTag(317, "Predictor", nil))
    ExtendedTags.Register(NewTag(318, "WhitePoint", nil))
    ExtendedTags.Register(NewTag(319, "PrimaryChromaticities", nil))
    ExtendedTags.Register(NewTag(321, "HalftoneHints", nil))
    ExtendedTags.Register(NewTag(322, "TileWidth", nil))
    ExtendedTags.Register(NewTag(323, "TileLength", nil))
    ExtendedTags.Register(NewTag(324, "TileOffsets", nil))
    ExtendedTags.Register(NewTag(325, "TileByteCounts", nil))
    ExtendedTags.Register(NewTag(326, "BadFaxLines", nil))
    ExtendedTags.Register(NewTag(327, "CleanFaxData", nil))
    ExtendedTags.Register(NewTag(328, "ConsecutiveBadFaxLines", nil))
    ExtendedTags.Register(NewTag(330, "SubIFDs", nil))
    ExtendedTags.Register(NewTag(332, "InkSet", nil))
    ExtendedTags.Register(NewTag(333, "InkNames", nil))
    ExtendedTags.Register(NewTag(334, "NumberOfInks", nil))
    ExtendedTags.Register(NewTag(336, "DotRange", nil))
    ExtendedTags.Register(NewTag(337, "TargetPrinter", nil))
    ExtendedTags.Register(NewTag(339, "SampleFormat", nil))
    ExtendedTags.Register(NewTag(340, "SMinSampleValue", nil))
    ExtendedTags.Register(NewTag(341, "SMaxSampleValue", nil))
    ExtendedTags.Register(NewTag(342, "TransferRange", nil))
    ExtendedTags.Register(NewTag(343, "ClipPath", nil))
    ExtendedTags.Register(NewTag(344, "XClipPathUnits", nil))
    ExtendedTags.Register(NewTag(345, "YClipPathUnits", nil))
    ExtendedTags.Register(NewTag(346, "Indexed", nil))
    ExtendedTags.Register(NewTag(347, "JPEGTables", nil))
    ExtendedTags.Register(NewTag(351, "OPIProxy", nil))
    ExtendedTags.Register(NewTag(400, "GlobalParametersIFD", nil))
    ExtendedTags.Register(NewTag(401, "ProfileType", nil))
    ExtendedTags.Register(NewTag(402, "FaxProfile", nil))
    ExtendedTags.Register(NewTag(403, "CodingMethods", nil))
    ExtendedTags.Register(NewTag(404, "VersionYear", nil))
    ExtendedTags.Register(NewTag(405, "ModeNumber", nil))
    ExtendedTags.Register(NewTag(433, "Decode", nil))
    ExtendedTags.Register(NewTag(434, "DefaultImageColor", nil))
    ExtendedTags.Register(NewTag(512, "JPEGProc", nil))
    ExtendedTags.Register(NewTag(513, "JPEGInterchangeFormat", nil))
    ExtendedTags.Register(NewTag(514, "JPEGInterchangeFormatLength", nil))
    ExtendedTags.Register(NewTag(515, "JPEGRestartInterval", nil))
    ExtendedTags.Register(NewTag(517, "JPEGLosslessPredictors", nil))
    ExtendedTags.Register(NewTag(518, "JPEGPointTransforms", nil))
    ExtendedTags.Register(NewTag(519, "JPEGQTables", nil))
    ExtendedTags.Register(NewTag(520, "JPEGDCTables", nil))
    ExtendedTags.Register(NewTag(521, "JPEGACTables", nil))
    ExtendedTags.Register(NewTag(529, "YCbCrCoefficients", nil))
    ExtendedTags.Register(NewTag(530, "YCbCrSubSampling", nil))
    ExtendedTags.Register(NewTag(531, "YCbCrPositioning", nil))
    ExtendedTags.Register(NewTag(532, "ReferenceBlackWhite", nil))
    ExtendedTags.Register(NewTag(559, "StripRowCounts", nil))
    ExtendedTags.Register(NewTag(700, "XMP", nil))
    ExtendedTags.Register(NewTag(32781, "ImageID", nil))
    ExtendedTags.Register(NewTag(34732, "ImageLayer", nil))

    // Prevent further registration in extended.  If tags are missing, they
    // should be added here instead of added from the outside.
    ExtendedTags.Lock()

    DefaultTagSpace.RegisterTagSet(ExtendedTags)
}
