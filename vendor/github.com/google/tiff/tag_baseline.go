// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

// TODO: Pass in the slice of valid FieldType for each tag.

var BaselineTags = NewTagSet("Baseline", 1, 64999)

func init() {
    BaselineTags.Register(NewTag(254, "NewSubfileType", nil))
    BaselineTags.Register(NewTag(255, "SubfileType", nil))
    BaselineTags.Register(NewTag(256, "ImageWidth", nil))
    BaselineTags.Register(NewTag(257, "ImageLength", nil))
    BaselineTags.Register(NewTag(258, "BitsPerSample", nil))
    BaselineTags.Register(NewTag(259, "Compression", nil))
    BaselineTags.Register(NewTag(262, "PhotometricInterpretation", nil))
    BaselineTags.Register(NewTag(263, "Threshholding", nil))
    BaselineTags.Register(NewTag(264, "CellWidth", nil))
    BaselineTags.Register(NewTag(265, "CellLength", nil))
    BaselineTags.Register(NewTag(266, "FillOrder", nil))
    BaselineTags.Register(NewTag(270, "ImageDescription", nil))
    BaselineTags.Register(NewTag(271, "Make", nil))
    BaselineTags.Register(NewTag(272, "Model", nil))
    BaselineTags.Register(NewTag(273, "StripOffsets", nil))
    BaselineTags.Register(NewTag(274, "Orientation", nil))
    BaselineTags.Register(NewTag(277, "SamplesPerPixel", nil))
    BaselineTags.Register(NewTag(278, "RowsPerStrip", nil))
    BaselineTags.Register(NewTag(279, "StripByteCounts", nil))
    BaselineTags.Register(NewTag(280, "MinSampleValue", nil))
    BaselineTags.Register(NewTag(281, "MaxSampleValue", nil))
    BaselineTags.Register(NewTag(282, "XResolution", nil))
    BaselineTags.Register(NewTag(283, "YResolution", nil))
    BaselineTags.Register(NewTag(284, "PlanarConfiguration", nil))
    BaselineTags.Register(NewTag(288, "FreeOffsets", nil))
    BaselineTags.Register(NewTag(289, "FreeByteCounts", nil))
    BaselineTags.Register(NewTag(290, "GrayResponseUnit", nil))
    BaselineTags.Register(NewTag(291, "GrayResponseCurve", nil))
    BaselineTags.Register(NewTag(296, "ResolutionUnit", nil))
    BaselineTags.Register(NewTag(305, "Software", nil))
    BaselineTags.Register(NewTag(306, "DateTime", nil))
    BaselineTags.Register(NewTag(315, "Artist", nil))
    BaselineTags.Register(NewTag(316, "HostComputer", nil))
    BaselineTags.Register(NewTag(320, "ColorMap", nil))
    BaselineTags.Register(NewTag(338, "ExtraSamples", nil))
    BaselineTags.Register(NewTag(33432, "Copyright", nil))

    // Prevent further registration in baseline.  If tags are missing, they
    // should be added here instead of added from the outside.
    BaselineTags.Lock()

    DefaultTagSpace.RegisterTagSet(BaselineTags)
}
