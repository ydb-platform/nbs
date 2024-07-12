# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Definition of gen_maven_jar_rules. """

load("//tools:java_single_jar.bzl", "java_single_jar")
load("//tools:javadoc.bzl", "javadoc_library")

_EXTERNAL_JAVADOC_LINKS = [
    "https://docs.oracle.com/javase/7/docs/api/",
    "https://developer.android.com/reference/",
]

_TINK_PACKAGES = [
    "com.google.crypto.tink",
]

def gen_maven_jar_rules(
        name,
        deps = [],
        root_packages = _TINK_PACKAGES,
        exclude_packages = [],
        doctitle = "",
        android_api_level = 23,
        bottom_text = "",
        external_javadoc_links = _EXTERNAL_JAVADOC_LINKS):
    """
    Generates rules that generate Maven jars for a given package.

    Args:
      name: Given a name, this function generates 3 rules: a compiled package
        name.jar, a source package name-src.jar and a Javadoc package
        name-javadoc.jar.
      deps: A combination of the deps of java_single_jar and javadoc_library
      root_packages: See javadoc_library
      exclude_packages: See javadoc_library
      doctitle: See javadoc_library
      android_api_level: See javadoc_library
      bottom_text: See javadoc_library
      external_javadoc_links: See javadoc_library
    """

    java_single_jar(
        name = name,
        deps = deps,
        root_packages = root_packages,
    )

    source_jar_name = name + "-src"
    java_single_jar(
        name = source_jar_name,
        deps = deps,
        root_packages = root_packages,
        source_jar = True,
    )

    javadoc_name = name + "-javadoc"
    javadoc_library(
        name = javadoc_name,
        deps = deps,
        root_packages = root_packages,
        srcs = [":%s" % source_jar_name],
        doctitle = doctitle,
        exclude_packages = exclude_packages,
        android_api_level = android_api_level,
        bottom_text = bottom_text,
        external_javadoc_links = external_javadoc_links,
    )
