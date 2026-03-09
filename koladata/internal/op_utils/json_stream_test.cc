// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "koladata/internal/op_utils/json_stream.h"

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>

#include "gtest/gtest.h"
#include "absl/types/span.h"
#include "koladata/internal/op_utils/stream_processor_state.pb.h"

namespace koladata::internal {
namespace {

template <typename ProcessorT, typename... OptionsT>
std::string RunProcessor(std::string input, const OptionsT&... options) {
  ProcessorT processor(options...);
  auto [output, end] = processor.Process(input, true);
  EXPECT_TRUE(end);

  std::string byte_streamed_output;
  processor.Reset();
  std::string state = processor.ToState();
  bool end_of_output = false;
  for (char c : input) {
    std::string input_chunk;
    input_chunk.push_back(c);
    {
      ProcessorT tmp_processor(options...);
      EXPECT_TRUE(tmp_processor.LoadState(state));
      std::string output_chunk;
      std::tie(output_chunk, end_of_output) =
          tmp_processor.Process(input_chunk, false);
      byte_streamed_output += output_chunk;
      state = tmp_processor.ToState();
      if (end_of_output) {
        break;
      }
    }
  }
  if (!end_of_output) {
    ProcessorT tmp_processor(options...);
    EXPECT_TRUE(tmp_processor.LoadState(state));
    auto [end_output, end_flag] = tmp_processor.Process("", true);
    EXPECT_TRUE(end_flag);
    byte_streamed_output += end_output;
  }

  EXPECT_EQ(byte_streamed_output, output);

  return output;
}

constexpr JsonSalvageOptions kDefaultJsonSalvageOptions = {
    .allow_nan = false,
    .ensure_ascii = false,
    .max_depth = 100,
};

constexpr JsonSalvageOptions kEnsureAsciiJsonSalvageOptions = {
    .allow_nan = false,
    .ensure_ascii = true,
    .max_depth = 100,
};

constexpr JsonSalvageOptions kAllowNanJsonSalvageOptions = {
    .allow_nan = true,
    .ensure_ascii = false,
    .max_depth = 100,
};

std::string RunSalvage(std::string input, const JsonSalvageOptions& options =
                                              kDefaultJsonSalvageOptions) {
  return RunProcessor<JsonSalvageStreamProcessor, JsonSalvageOptions>(input,
                                                                      options);
}

TEST(JsonStreamTest, SalvageStreamProcessorValidJson) {
  EXPECT_EQ(RunSalvage(""), "");
  EXPECT_EQ(RunSalvage("null"), "null");
  EXPECT_EQ(RunSalvage("true"), "true");
  EXPECT_EQ(RunSalvage("false"), "false");
  EXPECT_EQ(RunSalvage("0"), "0");
  EXPECT_EQ(RunSalvage("1"), "1");
  EXPECT_EQ(RunSalvage("-1"), "-1");
  EXPECT_EQ(RunSalvage("0.0"), "0.0");
  EXPECT_EQ(RunSalvage("0.5"), "0.5");
  EXPECT_EQ(RunSalvage("1.5"), "1.5");
  EXPECT_EQ(RunSalvage("-0.5"), "-0.5");
  EXPECT_EQ(RunSalvage("-1.5"), "-1.5");
  EXPECT_EQ(RunSalvage("0.0e0"), "0.0e0");
  EXPECT_EQ(RunSalvage("0.0e+0"), "0.0e+0");
  EXPECT_EQ(RunSalvage("0.0e-0"), "0.0e-0");
  EXPECT_EQ(RunSalvage("0e0"), "0e0");
  EXPECT_EQ(RunSalvage("0e-0"), "0e-0");
  EXPECT_EQ(RunSalvage("0e+0"), "0e+0");
  EXPECT_EQ(RunSalvage("1e100"), "1e100");
  EXPECT_EQ(RunSalvage("1e-100"), "1e-100");
  EXPECT_EQ(RunSalvage("1e+100"), "1e+100");
  EXPECT_EQ(RunSalvage("123.456e789"), "123.456e789");
  EXPECT_EQ(
      RunSalvage(
          "62302303891805340889130936088901407870066683052978661211675247124802"
          "24062798795284344852331075696347236418294203078437869057448630895929"
          "15854407457543913287819999913057019313870887275682423675743947927"),
      "623023038918053408891309360889014078700666830529786612116752471248022406"
      "279879528434485233107569634723641829420307843786905744863089592915854407"
      "457543913287819999913057019313870887275682423675743947927");
  EXPECT_EQ(
      RunSalvage("1030541808740199603499876866000153749572039622019058299928215"
                 "1713682590638133055005292735058819464876."
                 "6429973272012106081676174457299671868094444157442462036306814"
                 "0448515660259518722102551751709055957792e84792320588661664766"
                 "8005906681471239960534669959508339683692278089823445350301719"
                 "01840884058045093129"),
      "103054180874019960349987686600015374957203962201905829992821517136825906"
      "38133055005292735058819464876."
      "642997327201210608167617445729967186809444415744246203630681404485156602"
      "59518722102551751709055957792e847923205886616647668005906681471239960534"
      "66995950833968369227808982344535030171901840884058045093129");
  EXPECT_EQ(RunSalvage("\"\""), "\"\"");
  EXPECT_EQ(RunSalvage(" \"\" "), "\"\"");
  EXPECT_EQ(RunSalvage("\"abc\""), "\"abc\"");
  EXPECT_EQ(RunSalvage("\"*\""), "\"*\"");
  EXPECT_EQ(RunSalvage("\"\\\\\\\"\\b\\f\\n\\r\\t\""),
            "\"\\\\\\\"\\b\\f\\n\\r\\t\"");
  EXPECT_EQ(RunSalvage("\"\\u000a\""), "\"\\n\"");
  EXPECT_EQ(RunSalvage("\"\\u0022\""), "\"\\\"\"");
  EXPECT_EQ(RunSalvage("\"\\u005c\""), "\"\\\\\"");
  EXPECT_EQ(RunSalvage("\" \""), "\" \"");
  EXPECT_EQ(RunSalvage("\"½\""), "\"½\"");
  EXPECT_EQ(RunSalvage("\"\\u00bd\""), "\"½\"");
  EXPECT_EQ(RunSalvage("\"½\"", kEnsureAsciiJsonSalvageOptions), "\"\\u00bd\"");
  EXPECT_EQ(RunSalvage("\"\\u00bd\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\u00bd\"");
  EXPECT_EQ(RunSalvage("\"♠\""), "\"♠\"");
  EXPECT_EQ(RunSalvage("\"\\u2660\""), "\"♠\"");
  EXPECT_EQ(RunSalvage("\"♠\"", kEnsureAsciiJsonSalvageOptions), "\"\\u2660\"");
  EXPECT_EQ(RunSalvage("\"\\u2660\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\u2660\"");
  EXPECT_EQ(RunSalvage("\"😊\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\ud83d\\ude0a\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"😊\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ud83d\\ude0a\"");
  EXPECT_EQ(RunSalvage("\"\\ud83d\\ude0a\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ud83d\\ude0a\"");
  EXPECT_EQ(RunSalvage("[]"), "[]");
  EXPECT_EQ(RunSalvage("[true,false,null]"), "[true,false,null]");
  EXPECT_EQ(RunSalvage("[true, false, null]"), "[true,false,null]");
  EXPECT_EQ(RunSalvage("[ true, false, null ]"), "[true,false,null]");
  EXPECT_EQ(RunSalvage("[[],[[],[]]]"), "[[],[[],[]]]");
  EXPECT_EQ(RunSalvage("{}"), "{}");
  EXPECT_EQ(RunSalvage("{\"x\":\"y\"}"), "{\"x\":\"y\"}");
  EXPECT_EQ(RunSalvage("{\"x\":\"y\",\"z\":true}"), "{\"x\":\"y\",\"z\":true}");
  EXPECT_EQ(RunSalvage("{\"x\":{\"y\":\"z\"}}"), "{\"x\":{\"y\":\"z\"}}");

  EXPECT_EQ(
      RunSalvage(R"json(
  {
    "foo": [1, null, "bar", {}],
    "coords": {
      "x": 123.45,
      "y": -111.1e2,
      "z": null
    }
  }
  )json"),
      R"json({"foo":[1,null,"bar",{}],"coords":{"x":123.45,"y":-111.1e2,"z":null}})json");

  EXPECT_EQ(RunSalvage("0 0 0"), "0\n0\n0");
  EXPECT_EQ(RunSalvage("null null null"), "null\nnull\nnull");
  EXPECT_EQ(RunSalvage("[] [[]] [[[]]]"), "[]\n[[]]\n[[[]]]");
}

constexpr auto kRealisticJsonDocument = R"json(
{
  "metadata": {
    "id": "7f8b-4c21-9a03",
    "version": 2.0,
    "active": true,
    "tags": ["production", "api-v2", "nesting-test"],
    "checksum": null
  },
  "deep_nesting": {
    "level_1": {
      "level_2": {
        "level_3": [
          {
            "node_type": "leaf",
            "coordinates": [34.0522, -118.2437],
            "data_stream": [
              [1, 0, 0],
              [0, 1, 0],
              [0, 0, 1]
            ]
          },
          {
            "node_type": "branch",
            "recursive_ref": {
              "depth_limit": 10,
              "overflow": false
            }
          }
        ]
      }
    }
  },
  "data_types_showcase": {
    "strings": {
      "unicode": "Guten Tag, 世界",
      "escaped": "Line one\nLine two with \"quotes\" and a backslash: \\",
      "empty": ""
    },
    "numbers": {
      "integer": 42,
      "negative": -128,
      "floating_point": 3.1415926535,
      "scientific_notation": 6.022e23,
      "small_notation": 1.602e-19
    },
    "booleans_and_null": [true, false, null],
    "mixed_array": [
      100,
      "text",
      {"key": "value"},
      [1, 2, 3],
      null,
      true
    ]
  },
  "system_logs": [
    {
      "timestamp": "2026-02-26T08:38:37Z",
      "severity": "INFO",
      "message": "System check complete",
      "details": {
        "uptime": 86400,
        "load_averages": [0.15, 0.22, 0.05]
      }
    },
    {
      "timestamp": "2026-02-26T08:45:00Z",
      "severity": "WARNING",
      "message": "Memory threshold reached",
      "metrics": {
        "used_mb": 15420,
        "total_mb": 16384,
        "percentage": 94.12
      }
    }
  ],
  "configuration_overrides": {
    "feature_flags": {
      "beta_ui": true,
      "dark_mode": false,
      "experimental_engine": null
    },
    "user_preferences": {
      "theme": "nebula-dark",
      "font_size": 14,
      "notifications": {
        "email": true,
        "push": false,
        "sms": false
      }
    }
  }
}
)json";

// Formatted using python
// json.dumps(x, indent=None, separators=',:', ensure_ascii=False)
// with one extraneous `+` removed from `6.022e+23`.
constexpr auto kRealisticJsonDocumentWithoutWhitespace =
    R"json({"metadata":{"id":"7f8b-4c21-9a03","version":2.0,"active":true,"tags":["production","api-v2","nesting-test"],"checksum":null},"deep_nesting":{"level_1":{"level_2":{"level_3":[{"node_type":"leaf","coordinates":[34.0522,-118.2437],"data_stream":[[1,0,0],[0,1,0],[0,0,1]]},{"node_type":"branch","recursive_ref":{"depth_limit":10,"overflow":false}}]}}},"data_types_showcase":{"strings":{"unicode":"Guten Tag, 世界","escaped":"Line one\nLine two with \"quotes\" and a backslash: \\","empty":""},"numbers":{"integer":42,"negative":-128,"floating_point":3.1415926535,"scientific_notation":6.022e23,"small_notation":1.602e-19},"booleans_and_null":[true,false,null],"mixed_array":[100,"text",{"key":"value"},[1,2,3],null,true]},"system_logs":[{"timestamp":"2026-02-26T08:38:37Z","severity":"INFO","message":"System check complete","details":{"uptime":86400,"load_averages":[0.15,0.22,0.05]}},{"timestamp":"2026-02-26T08:45:00Z","severity":"WARNING","message":"Memory threshold reached","metrics":{"used_mb":15420,"total_mb":16384,"percentage":94.12}}],"configuration_overrides":{"feature_flags":{"beta_ui":true,"dark_mode":false,"experimental_engine":null},"user_preferences":{"theme":"nebula-dark","font_size":14,"notifications":{"email":true,"push":false,"sms":false}}}})json";

TEST(JsonStreamTest, SalvageStreamProcessorRealisticJsonDocument) {
  EXPECT_EQ(RunSalvage(kRealisticJsonDocument),
            kRealisticJsonDocumentWithoutWhitespace);
}

constexpr auto kRealisticJson5Document = R"json5(
/*
 * Fictional Game Configuration File
 * This document demonstrates all major JSON5 syntax features.
 * Unlike standard JSON, JSON5 allows multi-line comments.
 */
{
  // 1. Unquoted Keys: Object keys don't need quotes if they are valid identifiers
  title: 'Epic Fantasy Adventure',

  // 2. Single Quotes: Strings can be wrapped in single quotes
  environment: 'production',
  isDemo: false,

  // 3. Multi-line Strings: Strings can span multiple lines by escaping the newline
  description: 'This is a significantly long description \
that spans across multiple lines in the document \
to cleanly demonstrate JSON5 string continuations \
without needing explicit \\n characters in the source.',

  // 4. Numeric formats: JSON5 supports several new ways to write numbers
  stats: {
    maxPlayers: +8,          // Explicit positive sign
    baseHealth: 150.,        // Trailing decimal point
    damageMultiplier: .75,   // Leading decimal point
    magicHexCode: 0xCAFEBABE,// Hexadecimal numbers
    levelCap: Infinity,      // Positive Infinity
    negativeLimit: -Infinity,// Negative Infinity
    unknownValue: NaN,       // Not a Number (NaN)
  },                         // <-- Trailing comma in object!

  // 5. Arrays with trailing commas
  features: [
    'Single-player',
    'Multi-player',
    'Co-op',                 // <-- Trailing comma in array!
  ],

  // 6. Nested structures mixing standard JSON and JSON5 syntax
  characters: {
    'Hero': {
      class: 'Paladin',
      inventory: ['sword', 'shield', 'potion',],
    },
    "Villain": {
      class: "Necromancer",
      inventory: ["staff", "skull"], // Standard JSON still perfectly valid
    },
  },

  /*
   * 7. Additional configuration to expand file size.
   * JSON5 ignores all this extra whitespace and formatting.
   */
  networkConfig: {
    timeoutMs: 5000.,
    retryCount: +3,
    useProxy: false,
    endpoints: [
      'https://api.game-server-1.com/v1',
      'https://api.game-server-2.com/v1',
    ],
  },
}
)json5";

constexpr auto kRealisticJson5DocumentConvertedWithoutWhitespace =
    R"json({"title":"Epic Fantasy Adventure","environment":"production","isDemo":false,"description":"This is a significantly long description that spans across multiple lines in the document to cleanly demonstrate JSON5 string continuations without needing explicit \\n characters in the source.","stats":{"maxPlayers":8,"baseHealth":150.0,"damageMultiplier":0.75,"magicHexCode":3405691582,"levelCap":Infinity,"negativeLimit":-Infinity,"unknownValue":NaN},"features":["Single-player","Multi-player","Co-op"],"characters":{"Hero":{"class":"Paladin","inventory":["sword","shield","potion"]},"Villain":{"class":"Necromancer","inventory":["staff","skull"]}},"networkConfig":{"timeoutMs":5000.0,"retryCount":3,"useProxy":false,"endpoints":["https://api.game-server-1.com/v1","https://api.game-server-2.com/v1"]}})json";

TEST(JsonStreamTest, SalvageStreamProcessorRealisticJson5Document) {
  EXPECT_EQ(RunSalvage(kRealisticJson5Document, kAllowNanJsonSalvageOptions),
            kRealisticJson5DocumentConvertedWithoutWhitespace);
}

TEST(JsonStreamTest, SalvageStreamProcessorValidJson5) {
  EXPECT_EQ(RunSalvage("// a comment\n123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r\n123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\n// another comment\n123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r// another comment\r123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r\n// another comment\r\n123"), "123");

  EXPECT_EQ(RunSalvage("/* a comment\n and **another** comment*/123"), "123");

  EXPECT_EQ(RunSalvage(".0"), "0.0");
  EXPECT_EQ(RunSalvage("0."), "0.0");
  EXPECT_EQ(RunSalvage("1."), "1.0");
  EXPECT_EQ(RunSalvage("-.0"), "-0.0");
  EXPECT_EQ(RunSalvage("+.0"), "0.0");
  EXPECT_EQ(RunSalvage("-0."), "-0.0");
  EXPECT_EQ(RunSalvage("-1."), "-1.0");
  EXPECT_EQ(RunSalvage("+0."), "0.0");
  EXPECT_EQ(RunSalvage("+1."), "1.0");

  EXPECT_EQ(RunSalvage("0x1234"), "4660");
  EXPECT_EQ(RunSalvage("-0x1234"), "-4660");
  EXPECT_EQ(RunSalvage("0X1234"), "4660");

  EXPECT_EQ(RunSalvage("0x123456789abcdef"), "81985529216486895");
  EXPECT_EQ(RunSalvage("0x123456789aBcDeF"), "81985529216486895");
  EXPECT_EQ(RunSalvage("0x123456789ABCDEF"), "81985529216486895");

  EXPECT_EQ(RunSalvage("-Infinity"), "\"-Infinity\"");
  EXPECT_EQ(RunSalvage("+Infinity"), "\"+Infinity\"");
  EXPECT_EQ(RunSalvage("Infinity"), "\"Infinity\"");
  EXPECT_EQ(RunSalvage("NaN"), "\"NaN\"");
  EXPECT_EQ(RunSalvage("-NaN"), "\"-NaN\"");
  EXPECT_EQ(RunSalvage("+NaN"), "\"+NaN\"");

  EXPECT_EQ(RunSalvage("-Infinity", kAllowNanJsonSalvageOptions), "-Infinity");
  EXPECT_EQ(RunSalvage("+Infinity", kAllowNanJsonSalvageOptions), "Infinity");
  EXPECT_EQ(RunSalvage("Infinity", kAllowNanJsonSalvageOptions), "Infinity");
  EXPECT_EQ(RunSalvage("NaN", kAllowNanJsonSalvageOptions), "NaN");
  EXPECT_EQ(RunSalvage("-NaN", kAllowNanJsonSalvageOptions), "NaN");
  EXPECT_EQ(RunSalvage("+NaN", kAllowNanJsonSalvageOptions), "NaN");

  EXPECT_EQ(RunSalvage("''"), "\"\"");
  EXPECT_EQ(RunSalvage("' '"), "\" \"");
  EXPECT_EQ(RunSalvage("'\"'"), "\"\\\"\"");
  EXPECT_EQ(RunSalvage("'\\''"), "\"'\"");
  EXPECT_EQ(RunSalvage("'' ''"), "\"\"\n\"\"");

  // https://spec.json5.org/#escapes (for Line Terminators)
  EXPECT_EQ(RunSalvage("'a\\\nb'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\rb'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\r\nb'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\u2028b'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\u2029b'"), "\"ab\"");

  EXPECT_EQ(RunSalvage("\"\\x42\""), "\"B\"");
  EXPECT_EQ(RunSalvage("\"\\x12\""), "\"\\u0012\"");
  EXPECT_EQ(RunSalvage("\"\\0\""), "\"\\u0000\"");

  EXPECT_EQ(RunSalvage("[1,2,3,]"), "[1,2,3]");

  EXPECT_EQ(RunSalvage("{foo:true,bar:false}"), "{\"foo\":true,\"bar\":false}");

  // https://spec.json5.org/#white-space
  EXPECT_EQ(RunSalvage("123\u0009456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000a456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000b456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000c456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000d456"), "123\n456");
  EXPECT_EQ(RunSalvage("123 456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u00a0456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u2028456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u2029456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\ufeff456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u2009456"), "123\n456");  // U+2009 THIN SPACE
  EXPECT_EQ(RunSalvage("123\u3000456"),
            "123\n456");  // U+2000 IDEOGRAPHIC SPACE
}

TEST(JsonStreamTest, SalvageStreamProcessorValidPythonOrJavascript) {
  EXPECT_EQ(RunSalvage("# a comment\n123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r\n123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\n# another comment\n123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r# another comment\r123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r\n# another comment\r\n123"), "123");

  EXPECT_EQ(RunSalvage("None"), "null");
  EXPECT_EQ(RunSalvage("True"), "true");
  EXPECT_EQ(RunSalvage("False"), "false");

  EXPECT_EQ(RunSalvage("0"), "0");
  EXPECT_EQ(RunSalvage("00"), "0");
  EXPECT_EQ(RunSalvage("000"), "0");
  EXPECT_EQ(RunSalvage("0_00"), "0");
  EXPECT_EQ(RunSalvage("00_0"), "0");
  EXPECT_EQ(RunSalvage("000_"), "0");

  EXPECT_EQ(RunSalvage("0x_1234"), "4660");
  EXPECT_EQ(RunSalvage("0x12_34"), "4660");
  EXPECT_EQ(RunSalvage("0x1234_"), "4660");
  EXPECT_EQ(RunSalvage("0xDead_beef"), "3735928559");

  EXPECT_EQ(RunSalvage("3.14_15_93"), "3.141593");

  EXPECT_EQ(RunSalvage("0o123"), "83");
  EXPECT_EQ(RunSalvage("-0o123"), "-83");
  EXPECT_EQ(RunSalvage("0o_123"), "83");
  EXPECT_EQ(RunSalvage("0o1_23"), "83");
  EXPECT_EQ(RunSalvage("0o123_"), "83");
  EXPECT_EQ(RunSalvage("0o01234567"), "342391");

  EXPECT_EQ(RunSalvage("0b1011"), "11");
  EXPECT_EQ(RunSalvage("-0b1011"), "-11");
  EXPECT_EQ(RunSalvage("0b_1011"), "11");
  EXPECT_EQ(RunSalvage("0b10_11"), "11");
  EXPECT_EQ(RunSalvage("0b1011_"), "11");

  EXPECT_EQ(RunSalvage("123n"), "123");
  EXPECT_EQ(RunSalvage("123l"), "123");
  EXPECT_EQ(RunSalvage("123L"), "123");
  EXPECT_EQ(RunSalvage("0x123n"), "291");
  EXPECT_EQ(RunSalvage("0x123l"), "291");
  EXPECT_EQ(RunSalvage("0x123L"), "291");
  EXPECT_EQ(RunSalvage("0o123n"), "83");
  EXPECT_EQ(RunSalvage("0o123l"), "83");
  EXPECT_EQ(RunSalvage("0o123L"), "83");

  EXPECT_EQ(RunSalvage("\"\b\f\n\r\t\""), "\"\\b\\f\\n\\r\\t\"");

  EXPECT_EQ(RunSalvage("``"), "\"\"");
  EXPECT_EQ(RunSalvage("` `"), "\" \"");
  EXPECT_EQ(RunSalvage("`\"`"), "\"\\\"\"");
  EXPECT_EQ(RunSalvage("`\\``"), "\"`\"");

  EXPECT_EQ(RunSalvage("''''''"), "\"\"");
  EXPECT_EQ(RunSalvage("''' '''"), "\" \"");
  EXPECT_EQ(RunSalvage("''' ' '''"), "\" ' \"");
  EXPECT_EQ(RunSalvage("''' '' '''"), "\" '' \"");
  EXPECT_EQ(RunSalvage("'''\n\n\n'''"), "\"\\n\\n\\n\"");
  EXPECT_EQ(RunSalvage("'''a\\\nb'''"), "\"ab\"");

  EXPECT_EQ(RunSalvage("\"\"\"\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\" \"\"\""), "\" \"");
  EXPECT_EQ(RunSalvage("\"\"\" \" \"\"\""), "\" \\\" \"");
  EXPECT_EQ(RunSalvage("\"\"\" \"\" \"\"\""), "\" \\\"\\\" \"");
  EXPECT_EQ(RunSalvage("\"\"\"\n\n\n\"\"\""), "\"\\n\\n\\n\"");

  EXPECT_EQ(RunSalvage("b''"), "\"\"");
  EXPECT_EQ(RunSalvage("b'abc'"), "\"abc\"");
  EXPECT_EQ(RunSalvage("b'\\x00\\xFF'"), "\"\\u0000\u00ff\"");
  EXPECT_EQ(RunSalvage("b'\\x00\\xFF'", kEnsureAsciiJsonSalvageOptions),
            "\"\\u0000\\u00ff\"");

  EXPECT_EQ(RunSalvage("u''"), "\"\"");
  EXPECT_EQ(RunSalvage("u'abc'"), "\"abc\"");

  EXPECT_EQ(RunSalvage("b'''abc'''"), "\"abc\"");
  EXPECT_EQ(RunSalvage("b\"\"\"abc\"\"\""), "\"abc\"");
  EXPECT_EQ(RunSalvage("u'''abc'''"), "\"abc\"");
  EXPECT_EQ(RunSalvage("u\"\"\"abc\"\"\""), "\"abc\"");

  EXPECT_EQ(RunSalvage("\"\\\\'\\\"\\a\\b\\f\\n\\r\\t\\v\""),
            "\"\\\\'\\\"\\u0007\\b\\f\\n\\r\\t\\u000b\"");

  EXPECT_EQ(RunSalvage("\"\\1\""), "\"\\u0001\"");
  EXPECT_EQ(RunSalvage("\"\\00\""), "\"\\u0000\"");
  EXPECT_EQ(RunSalvage("\"\\01\""), "\"\\u0001\"");
  EXPECT_EQ(RunSalvage("\"\\21\""), "\"\\u0011\"");
  EXPECT_EQ(RunSalvage("\"\\000\""), "\"\\u0000\"");
  EXPECT_EQ(RunSalvage("\"\\001\""), "\"\\u0001\"");
  EXPECT_EQ(RunSalvage("\"\\021\""), "\"\\u0011\"");
  EXPECT_EQ(RunSalvage("\"\\321\""), "\"\u00d1\"");
  EXPECT_EQ(RunSalvage("\"\\U0001f60a\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\u{1f60a}\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\u{01f60a}\""), "\"😊\"");

  EXPECT_EQ(RunSalvage("(1,2,3)"), "[1,2,3]");
}

TEST(JsonStreamTest, SalvageStreamProcessorInvalidUtf8) {
  EXPECT_EQ(RunSalvage("\xff"), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\xc2\xff"), "\"\ufffd\ufffd\"");
  EXPECT_EQ(RunSalvage("\xed\xa0\x80"), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\x80"), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\xc2"), "");  // Truncated by end of input.
  EXPECT_EQ(RunSalvage("\xc2\xc2"),
            "\"\ufffd\"");  // Truncated by end of input.
  EXPECT_EQ(RunSalvage("\xc0\x80"), "\"\ufffd\ufffd\"");    // Overlong.
  EXPECT_EQ(RunSalvage("\xe0\x8f\x80"), "\"\ufffd\"");      // Overlong.
  EXPECT_EQ(RunSalvage("\xf0\x8f\x80\x80"), "\"\ufffd\"");  // Overlong.
  EXPECT_EQ(RunSalvage("\xed\xa0\x80"), "\"\ufffd\"");      // Surrogate.

  EXPECT_EQ(RunSalvage("\"\xff\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc2\xff\""), "\"\ufffd\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\x80\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc2\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc2\xc2\""), "\"\ufffd\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc0\x80\""), "\"\ufffd\ufffd\"");  // Overlong.
  EXPECT_EQ(RunSalvage("\"\xe0\x8f\x80\""), "\"\ufffd\"");    // Overlong.
  EXPECT_EQ(RunSalvage("\"\xed\xa0\x80\""), "\"\ufffd\"");    // Surrogate.

  EXPECT_EQ(RunSalvage("[\xff]"), "[\"\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc2\xff]"), "[\"\ufffd\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\x80]"), "[\"\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc2]"), "[\"\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc2\xc2]"), "[\"\ufffd\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc0\x80]"), "[\"\ufffd\ufffd\"]");  // Overlong.
  EXPECT_EQ(RunSalvage("[\xe0\x8f\x80]"), "[\"\ufffd\"]");    // Overlong.
  EXPECT_EQ(RunSalvage("[\xed\xa0\x80]"), "[\"\ufffd\"]");    // Surrogate.
}

TEST(JsonStreamTest, SalvageStreamProcessorInvalidUtf16Surrogates) {
  EXPECT_EQ(RunSalvage("\"\\ud800\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\ud800?\""), "\"?\ufffd?\"");
  EXPECT_EQ(RunSalvage("\"\\udc00\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\udc00?\""), "\"?\ufffd?\"");

  EXPECT_EQ(RunSalvage("\"\\ud800\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\ud800?\"", kEnsureAsciiJsonSalvageOptions),
            "\"?\\ufffd?\"");
  EXPECT_EQ(RunSalvage("\"\\udc00\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\udc00?\"", kEnsureAsciiJsonSalvageOptions),
            "\"?\\ufffd?\"");
}

TEST(JsonStreamTest, SalvageStreamProcessorInvalidFormat) {
  EXPECT_EQ(RunSalvage("{"), "{}");
  EXPECT_EQ(RunSalvage("}"), "");
  EXPECT_EQ(RunSalvage("{}}"), "{}");
  EXPECT_EQ(RunSalvage("{}}}"), "{}");
  EXPECT_EQ(RunSalvage("{\"x\":"), "{\"x\":null}");
  EXPECT_EQ(RunSalvage("["), "[]");
  EXPECT_EQ(RunSalvage("]"), "");
  EXPECT_EQ(RunSalvage("[]]"), "[]");
  EXPECT_EQ(RunSalvage("[]]]"), "[]");
  EXPECT_EQ(RunSalvage("[1"), "[1]");
  EXPECT_EQ(RunSalvage("[1,2"), "[1,2]");
  EXPECT_EQ(RunSalvage("\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("'"), "\"\"");
  EXPECT_EQ(RunSalvage("'''"), "\"\"");
  EXPECT_EQ(RunSalvage("''''"), "\"\"");
  EXPECT_EQ(RunSalvage("'''''"), "\"\"");
  EXPECT_EQ(RunSalvage("`"), "\"\"");
  EXPECT_EQ(RunSalvage("\"\\"), "\"\"");
  EXPECT_EQ(RunSalvage("."), "0.0");
  EXPECT_EQ(RunSalvage(".e"), "0.0e0");
  EXPECT_EQ(RunSalvage("0.0e"), "0.0e0");
  EXPECT_EQ(RunSalvage("0.e"), "0.0e0");
  EXPECT_EQ(RunSalvage("#"), "");
  EXPECT_EQ(RunSalvage("//"), "");
  EXPECT_EQ(RunSalvage("/*"), "");
  EXPECT_EQ(RunSalvage("/* *"), "");
  EXPECT_EQ(RunSalvage("+/"), "\"+/\"");

  EXPECT_EQ(RunSalvage("00123"), "123");
  EXPECT_EQ(RunSalvage("0b01012"), "5\n2");
  EXPECT_EQ(RunSalvage("0o123456789"), "342391\n89");

  EXPECT_EQ(RunSalvage(".."), "0.0\n0.0");
  EXPECT_EQ(RunSalvage("0.."), "0.0\n0.0");
  EXPECT_EQ(RunSalvage("0.0."), "0.0\n0.0");
  EXPECT_EQ(RunSalvage("0.e."), "0.0e0\n0.0");
  EXPECT_EQ(RunSalvage("0.ee"), "0.0e0\n\"e\"");
  EXPECT_EQ(RunSalvage("000'"), "0");
  EXPECT_EQ(RunSalvage("0'00"), "0");
  EXPECT_EQ(RunSalvage("00'0"), "0");
  EXPECT_EQ(RunSalvage("000'"), "0");
  EXPECT_EQ(RunSalvage("00."), "0.0");
  EXPECT_EQ(RunSalvage("000."), "0.0");

  EXPECT_EQ(RunSalvage("/"), "\"/\"");
  EXPECT_EQ(RunSalvage("[/]"), "[\"/\"]");
  EXPECT_EQ(RunSalvage("/abc"), "\"/abc\"");
  EXPECT_EQ(RunSalvage("[/abc]"), "[\"/abc\"]");

  EXPECT_EQ(RunSalvage("t0"), "\"t0\"");
  EXPECT_EQ(RunSalvage("tr0"), "\"tr0\"");
  EXPECT_EQ(RunSalvage("tru0"), "\"tru0\"");
  EXPECT_EQ(RunSalvage("true0"), "true\n0");
  EXPECT_EQ(RunSalvage("t."), "\"t.\"");
  EXPECT_EQ(RunSalvage("tr."), "\"tr.\"");
  EXPECT_EQ(RunSalvage("tru."), "\"tru.\"");
  EXPECT_EQ(RunSalvage("true."), "true\n0.0");

  EXPECT_EQ(RunSalvage("abc[def"), "\"abc\"\n[\"def\"]");
  EXPECT_EQ(RunSalvage("abc{def"), "\"abc\"\n{\"def\":null}");
  EXPECT_EQ(RunSalvage("[abc[def"), "[\"abc\",[\"def\"]]");

  EXPECT_EQ(RunSalvage("\"\\w\""), "\"\\\\w\"");  // Not a real escape.
  EXPECT_EQ(RunSalvage("\"\\u42???\""), "\"B???\"");
  EXPECT_EQ(RunSalvage("\"\\xf???\""), "\"\\u000f???\"");
  EXPECT_EQ(RunSalvage("\"\\U1f60a???\""), "\"😊???\"");
  EXPECT_EQ(RunSalvage("\"\\U{1f60a}\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\Uaaaaaaaa\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\\u{aaaaaaaa}\""), "\"\ufffd\"");

  EXPECT_EQ(RunSalvage("{\"x\"}"), "{\"x\":null}");
  EXPECT_EQ(RunSalvage("{[1]:[2]}"), "{\"\":[1],\"\":[2]}");
  EXPECT_EQ(RunSalvage("{[1],[2]}"), "{\"\":[1],\"\":[2]}");
  EXPECT_EQ(RunSalvage("{{1}:{2}}"), "{\"\":{\"1\":null},\"\":{\"2\":null}}");
  EXPECT_EQ(RunSalvage("{[1]:\"a\"}"), "{\"\":[1],\"a\":null}");

  EXPECT_EQ(RunSalvage("[1 2 3]"), "[1,2,3]");
  EXPECT_EQ(RunSalvage("[,, ,1,,2,, ,,3,,]"), "[1,2,3]");
  EXPECT_EQ(RunSalvage("[,,,1, ,2: ::3,, ]"), "[1,2,3]");

  // Truncated to lower 64 bits.
  EXPECT_EQ(RunSalvage("0x123456789abcdef01"), "2541551405711093505");
  EXPECT_EQ(RunSalvage("0o12345670123456701234567"), "4140784126758566263");
  EXPECT_EQ(RunSalvage("0b01010101010101010101010101010101010101010101010101010"
                       "1010101010101"),
            "6148914691236517205");

  EXPECT_EQ(
      RunSalvage(
          // 200 '['s
          "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
          "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
          "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["),
      // Beyond max depth, treated as unquoted string, but still matched
      // fully afterward.
      "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
      "[[[[[[[[[[[[[[[[[[[[[[[[[[[[\"[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
      "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[\"]]]]]]]]]]]]"
      "]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"
      "]]]]]]]]]]]]]]]]");

  // Markdown-style code block fences aren't treated as backtick strings, and
  // are instead treated as unquoted strings containing only the ``` lines, so
  // that the content isn't captured as a single string instead of being parsed.
  EXPECT_EQ(RunSalvage("```json\n{\"x\": \"y\"}\n```\n"),
            "\"```json\"\n{\"x\":\"y\"}\n\"```\"");

  // Various "torture test" inputs contributed by a supernatural entity.
  EXPECT_EQ(RunSalvage("nulla[truep,Falsen,{1:None1}]"),
            "null\n\"a\"\n[true,\"p\",false,\"n\",{\"1\":null,\"1\":null}]");
  EXPECT_EQ(RunSalvage("\"'nested \\\" \\' \\`'\""), "\"'nested \\\" ' `'\"");
  EXPECT_EQ(RunSalvage("'foo\\\nbar' \\\n \"\"\"baz\\\rquux\"\"\""),
            "\"foobar\"\n\"\\\\\"\n\"bazquux\"");
  EXPECT_EQ(RunSalvage("foo'bar'`baz`\"quux\""),
            "\"foo'bar'`baz`\\\"quux\\\"\"");
  EXPECT_EQ(RunSalvage("1/*foo*/2"), "1\n2");
  EXPECT_EQ(RunSalvage("/* // foo */ bar // /* baz */"), "\"bar\"");
  EXPECT_EQ(RunSalvage("\x80 \xc0\x80 \xf0\x8f\x80\x80"),
            "\"\ufffd\"\n\"\ufffd\ufffd\"\n\"\ufffd\"");
}

TEST(JsonStreamTest, SalvageStreamProcessorMinimallyDelayed) {
  JsonSalvageStreamProcessor processor(kDefaultJsonSalvageOptions);
  // {"a": 123, "b\u2660c": ['''xyz''', 12.34e56]}

  auto p = [&](std::string_view chunk) {
    return std::get<0>(processor.Process(chunk, false));
  };

  // Note: separators (',' ':') are delayed slightly because it's impossible to
  // do removal of trailing commas without waiting for the next value or end of
  // container.
  EXPECT_EQ(p("{"), "{");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("a"), "a");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p(":"), "");
  EXPECT_EQ(p(" "), "");
  EXPECT_EQ(p("1"), ":1");
  EXPECT_EQ(p("2"), "2");
  EXPECT_EQ(p("3"), "3");
  EXPECT_EQ(p(","), "");
  EXPECT_EQ(p(" "), "");
  EXPECT_EQ(p("\""), ",\"");
  EXPECT_EQ(p("b"), "b");
  EXPECT_EQ(p("\\"), "");
  EXPECT_EQ(p("u"), "");
  EXPECT_EQ(p("2"), "");
  EXPECT_EQ(p("6"), "");
  EXPECT_EQ(p("6"), "");
  EXPECT_EQ(p("0"), "♠");
  EXPECT_EQ(p("c"), "c");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p(":"), "");
  EXPECT_EQ(p(" "), "");
  EXPECT_EQ(p("["), ":[");
  EXPECT_EQ(p("'"), "\"");
  EXPECT_EQ(p("'"), "");
  EXPECT_EQ(p("'"), "");
  EXPECT_EQ(p("x"), "x");
  EXPECT_EQ(p("y"), "y");
  EXPECT_EQ(p("z"), "z");
  EXPECT_EQ(p("'"), "");
  EXPECT_EQ(p("'"), "");
  EXPECT_EQ(p("'"), "\"");
  EXPECT_EQ(p(","), "");
  EXPECT_EQ(p(" "), "");
  EXPECT_EQ(p("1"), ",1");
  EXPECT_EQ(p("2"), "2");
  EXPECT_EQ(p("."), ".");
  EXPECT_EQ(p("3"), "3");
  EXPECT_EQ(p("4"), "4");
  EXPECT_EQ(p("e"), "e");
  EXPECT_EQ(p("5"), "5");
  EXPECT_EQ(p("6"), "6");
  EXPECT_EQ(p("]"), "]");
  EXPECT_EQ(p("}"), "}");
}

TEST(JsonStreamTest, SalvageStreamProcessorLoadStateFailure) {
  JsonSalvageStreamProcessor processor(kDefaultJsonSalvageOptions);

  EXPECT_FALSE(processor.LoadState(""));
  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  EXPECT_FALSE(processor.LoadState(
      JsonSalvageStreamProcessor(kAllowNanJsonSalvageOptions).ToState()));

  {
    JsonSalvageStateProto proto;
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    for (int i = 0; i < 4; ++i) {
      proto.mutable_utf8_buffer()->push_back('A');
    }
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    for (int i = 0; i < 101; ++i) {
      proto.add_container_stack(0);
    }
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_container_stack(3);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_container_stack(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    for (int i = 0; i < 101; ++i) {
      proto.add_buffer(0);
    }
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_buffer(0x110000);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_buffer(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.set_state(11);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunHead(std::string input, int64_t n) {
  return RunProcessor<JsonHeadStreamProcessor>(input, JsonHeadOptions{.n = n});
}

TEST(JsonStreamTest, HeadStreamProcessor) {
  EXPECT_EQ(RunHead("", 0), "");
  EXPECT_EQ(RunHead("", 1), "");
  EXPECT_EQ(RunHead("", 5), "");

  EXPECT_EQ(RunHead("123\n", 0), "");
  EXPECT_EQ(RunHead("123\n", 1), "123\n");
  EXPECT_EQ(RunHead("123\n", 2), "123\n");

  EXPECT_EQ(RunHead("1\n2\n3\n", 0), "");
  EXPECT_EQ(RunHead("1\n2\n3\n", 1), "1\n");
  EXPECT_EQ(RunHead("1\n2\n3\n", 2), "1\n2\n");
  EXPECT_EQ(RunHead("1\n2\n3\n", 3), "1\n2\n3\n");
  EXPECT_EQ(RunHead("1\n2\n3\n", 4), "1\n2\n3\n");
  EXPECT_EQ(RunHead("1\n2\n3\n", 100), "1\n2\n3\n");

  EXPECT_EQ(RunHead("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n", 1), "{\"a\":1}\n");
  EXPECT_EQ(RunHead("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n", 2),
            "{\"a\":1}\n{\"b\":2}\n");
  EXPECT_EQ(RunHead("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n", 3),
            "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");

  EXPECT_EQ(RunHead("[1,2,3]\n[4,5]\n", 1), "[1,2,3]\n");
  EXPECT_EQ(RunHead("[1,2,3]\n[4,5]\n", 2), "[1,2,3]\n[4,5]\n");

  EXPECT_EQ(RunHead("\"abc\\ndef\"\n\"ghi\"\n", 1), "\"abc\\ndef\"\n");
}

TEST(JsonStreamTest, HeadStreamProcessorEarlyEnd) {
  JsonHeadStreamProcessor processor({.n = 2});
  auto [out1, end1] = processor.Process("123", false);
  EXPECT_EQ(out1, "123");
  EXPECT_FALSE(end1);

  auto [out2, end2] = processor.Process("\n", false);
  EXPECT_EQ(out2, "\n");
  EXPECT_FALSE(end2);

  auto [out3, end3] = processor.Process("456", false);
  EXPECT_EQ(out3, "456");
  EXPECT_FALSE(end3);

  auto [out4, end4] = processor.Process("\n", false);
  EXPECT_EQ(out4, "\n");
  EXPECT_TRUE(end4);
}

TEST(JsonStreamTest, HeadStreamProcessorLoadStateFailure) {
  JsonHeadStreamProcessor processor({.n = 1});

  EXPECT_FALSE(processor.LoadState(""));
  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  EXPECT_FALSE(
      processor.LoadState(JsonHeadStreamProcessor({.n = 2}).ToState()));
  {
    JsonHeadStateProto proto;
    proto.set_n(1);
    proto.set_line_number(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunPrettify(std::string input, std::string indent_string = "  ") {
  return RunProcessor<JsonPrettifyStreamProcessor, JsonPrettifyOptions>(
      input, JsonPrettifyOptions{.indent_string = indent_string});
}

TEST(JsonStreamTest, PrettifyStreamProcessor) {
  // Top-level value handling.
  EXPECT_EQ(RunPrettify(""), "");
  EXPECT_EQ(RunPrettify("123"), "123");
  EXPECT_EQ(RunPrettify("\"abc\""), "\"abc\"");
  EXPECT_EQ(RunPrettify("123 456 789"), "123\n456\n789");
  EXPECT_EQ(RunPrettify("123\n456\n789"), "123\n456\n789");
  EXPECT_EQ(RunPrettify("123  456  789"), "123\n456\n789");
  EXPECT_EQ(RunPrettify("123   456   789"), "123\n456\n789");
  EXPECT_EQ(RunPrettify("\"abc\" \"def\" \"ghi\""),
            "\"abc\"\n\"def\"\n\"ghi\"");
  EXPECT_EQ(RunPrettify("\"abc\"\"def\"\"ghi\""), "\"abc\"\n\"def\"\n\"ghi\"");
  EXPECT_EQ(RunPrettify("\"abc\"\n\"def\"\n\"ghi\""),
            "\"abc\"\n\"def\"\n\"ghi\"");
  EXPECT_EQ(RunPrettify("{}"), "{}");
  EXPECT_EQ(RunPrettify("{}{}{}"), "{}\n{}\n{}");
  EXPECT_EQ(RunPrettify("{} {} {}"), "{}\n{}\n{}");
  EXPECT_EQ(RunPrettify("{}  {}  {}"), "{}\n{}\n{}");
  EXPECT_EQ(RunPrettify("{ }  { }  { }"), "{}\n{}\n{}");
  EXPECT_EQ(RunPrettify("[]"), "[]");
  EXPECT_EQ(RunPrettify("[][][]"), "[]\n[]\n[]");
  EXPECT_EQ(RunPrettify("[] [] []"), "[]\n[]\n[]");
  EXPECT_EQ(RunPrettify("[]  []  []"), "[]\n[]\n[]");
  EXPECT_EQ(RunPrettify("[ ]  [ ]  [ ]"), "[]\n[]\n[]");

  // String escapes.
  EXPECT_EQ(RunPrettify("[\"abc\"]"), "[\n  \"abc\"\n]");
  EXPECT_EQ(RunPrettify("[\"abc\\\\def\"]"), "[\n  \"abc\\\\def\"\n]");
  EXPECT_EQ(RunPrettify("[\"abc\\\"def\"]"), "[\n  \"abc\\\"def\"\n]");
  EXPECT_EQ(RunPrettify("[\"abc\\\\\"]"), "[\n  \"abc\\\\\"\n]");

  // Nonempty arrays and indentation.
  EXPECT_EQ(RunPrettify("[[]]"), "[\n  []\n]");
  EXPECT_EQ(RunPrettify("[ [ ] ] "), "[\n  []\n]");
  EXPECT_EQ(RunPrettify("[[[]]]"), "[\n  [\n    []\n  ]\n]");
  EXPECT_EQ(RunPrettify("[ [ [ ] ] ] "), "[\n  [\n    []\n  ]\n]");
  EXPECT_EQ(RunPrettify("[[],[],[]]"), "[\n  [],\n  [],\n  []\n]");
  EXPECT_EQ(RunPrettify("[ [ ] , [ ] , [ ] ] "), "[\n  [],\n  [],\n  []\n]");
  EXPECT_EQ(RunPrettify("[123,456,789]"), "[\n  123,\n  456,\n  789\n]");
  EXPECT_EQ(RunPrettify("[ 123 , 456 , 789 ] "), "[\n  123,\n  456,\n  789\n]");
  EXPECT_EQ(RunPrettify("[\"abc\",\"def\",\"ghi\"]"),
            "[\n  \"abc\",\n  \"def\",\n  \"ghi\"\n]");
  EXPECT_EQ(RunPrettify("[ \"abc\" , \"def\" , \"ghi\" ] "),
            "[\n  \"abc\",\n  \"def\",\n  \"ghi\"\n]");
  EXPECT_EQ(RunPrettify("[]\n[]\n[]\n"), "[]\n[]\n[]");
  EXPECT_EQ(RunPrettify("[ ]\n[ ]\n[ ]\n"), "[]\n[]\n[]");

  // Alternative indent strings.
  EXPECT_EQ(RunPrettify("[[]]", "\t"), "[\n\t[]\n]");
  EXPECT_EQ(RunPrettify("[[[]]]", "\t"), "[\n\t[\n\t\t[]\n\t]\n]");
  EXPECT_EQ(RunPrettify("[[],[],[]]", "\t"), "[\n\t[],\n\t[],\n\t[]\n]");
  EXPECT_EQ(RunPrettify("[[]]", "    "), "[\n    []\n]");
  EXPECT_EQ(RunPrettify("[[[]]]", "    "), "[\n    [\n        []\n    ]\n]");
  EXPECT_EQ(RunPrettify("[[],[],[]]", "    "),
            "[\n    [],\n    [],\n    []\n]");

  // Nonempty objects.
  EXPECT_EQ(RunPrettify("{\"key\":\"value\"}"), "{\n  \"key\": \"value\"\n}");
  EXPECT_EQ(RunPrettify("{ \"key\" : \"value\" } "),
            "{\n  \"key\": \"value\"\n}");
  EXPECT_EQ(RunPrettify("{\"a\":1,\"b\":2}"), "{\n  \"a\": 1,\n  \"b\": 2\n}");
  EXPECT_EQ(RunPrettify("{ \"a\" : 1 , \"b\" : 2 }"),
            "{\n  \"a\": 1,\n  \"b\": 2\n}");
  EXPECT_EQ(RunPrettify("{\"a\":\"x\",\"b\":\"y\"}"),
            "{\n  \"a\": \"x\",\n  \"b\": \"y\"\n}");
  EXPECT_EQ(RunPrettify("{ \"a\" : \"x\" , \"b\" : \"y\" } "),
            "{\n  \"a\": \"x\",\n  \"b\": \"y\"\n}");
  EXPECT_EQ(RunPrettify("{\"a\":[],\"b\":[],\"c\":[]}"),
            "{\n  \"a\": [],\n  \"b\": [],\n  \"c\": []\n}");
  EXPECT_EQ(RunPrettify("{ \"a\" : [ ] , \"b\" : [ ] , \"c\" : [ ] } "),
            "{\n  \"a\": [],\n  \"b\": [],\n  \"c\": []\n}");

  // Larger example. Expected string generated using json.dumps(x, indent=2).
  EXPECT_EQ(
      RunPrettify(
          R"json({"a":1,"b":"x","c":true,"d":null,"e":[2],"f":{"g":3}})json"),
      "{\n  \"a\": 1,\n  \"b\": \"x\",\n  \"c\": true,\n  \"d\": null,\n  "
      "\"e\": [\n    2\n  ],\n  \"f\": {\n    \"g\": 3\n  }\n}");
  EXPECT_EQ(
      RunPrettify(
          R"json({ "a" : 1 , "b" : "x" , "c" : true , "d" : null , "e" : [ 2 ] , "f" : { "g" : 3 } } )json"),
      "{\n  \"a\": 1,\n  \"b\": \"x\",\n  \"c\": true,\n  \"d\": null,\n  "
      "\"e\": [\n    2\n  ],\n  \"f\": {\n    \"g\": 3\n  }\n}");
}

TEST(JsonStreamTest, PrettifyStreamProcessorLoadStateFailure) {
  JsonPrettifyStreamProcessor processor({.indent_string = "  "});

  EXPECT_FALSE(processor.LoadState(""));
  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  EXPECT_FALSE(processor.LoadState(
      JsonPrettifyStreamProcessor({.indent_string = " "}).ToState()));
  {
    JsonPrettifyStateProto proto;
    proto.set_indent_string("  ");
    proto.set_container_depth(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunCompactify(std::string input) {
  return RunProcessor<JsonCompactifyStreamProcessor>(input);
}

TEST(JsonStreamTest, CompactifyStreamProcessor) {
  // Note: concatenated JSON without any intermediate whitespace is technically
  // invalid, but for containers and strings we make sure to separate it anyway.
  EXPECT_EQ(RunCompactify(""), "");
  EXPECT_EQ(RunCompactify("123"), "123\n");
  EXPECT_EQ(RunCompactify(" 123 "), "123\n");
  EXPECT_EQ(RunCompactify("123 456 789"), "123\n456\n789\n");
  EXPECT_EQ(RunCompactify("123  456  789"), "123\n456\n789\n");
  EXPECT_EQ(RunCompactify("\"abc\""), "\"abc\"\n");
  EXPECT_EQ(RunCompactify("\"abc\\\"def\\\\\"\"ghi\""),
            "\"abc\\\"def\\\\\"\n\"ghi\"\n");
  EXPECT_EQ(RunCompactify("\"abc def ghi\""), "\"abc def ghi\"\n");
  EXPECT_EQ(RunCompactify(" \"abc\" "), "\"abc\"\n");
  EXPECT_EQ(RunCompactify("\"abc\"\"def\"\"ghi\""),
            "\"abc\"\n\"def\"\n\"ghi\"\n");
  EXPECT_EQ(RunCompactify("\"abc\" \"def\" \"ghi\""),
            "\"abc\"\n\"def\"\n\"ghi\"\n");
  EXPECT_EQ(RunCompactify("\"abc\"123[]true"), "\"abc\"\n123\n[]\ntrue\n");
  EXPECT_EQ(RunCompactify("[][][]"), "[]\n[]\n[]\n");
  EXPECT_EQ(RunCompactify("[] [] []"), "[]\n[]\n[]\n");
  EXPECT_EQ(RunCompactify("{}{}{}"), "{}\n{}\n{}\n");
  EXPECT_EQ(RunCompactify(" { } { } { } "), "{}\n{}\n{}\n");
  EXPECT_EQ(RunCompactify("[][[]][[[],[]]]"), "[]\n[[]]\n[[[],[]]]\n");
  EXPECT_EQ(RunCompactify(" [ ] [ [ ] ] [ [ [ ] , [ ] ] ] "),
            "[]\n[[]]\n[[[],[]]]\n");

  EXPECT_EQ(RunCompactify("{\"key\": \"value\"}"), "{\"key\":\"value\"}\n");
  EXPECT_EQ(RunCompactify("{\"a\": 1, \"b\": 2}"), "{\"a\":1,\"b\":2}\n");
}

TEST(JsonStreamTest, CompactifyStreamProcessorLoadStateFailure) {
  JsonCompactifyStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  {
    JsonCompactifyStateProto proto;
    proto.set_container_depth(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunSelectNonemptyObjects(std::string input) {
  return RunProcessor<JsonSelectNonemptyObjectsStreamProcessor>(input);
}

TEST(JsonStreamTest, SelectNonemptyObjectsStreamProcessor) {
  EXPECT_EQ(RunSelectNonemptyObjects(""), "");
  EXPECT_EQ(RunSelectNonemptyObjects(
                "123\nnull\n{}\n{\"a\":\"b\"}\n[]\n[1,2,3]\n{\"c\":\"d\"}\n"),
            "{\"a\":\"b\"}\n{\"c\":\"d\"}\n");
}

TEST(JsonStreamTest, SelectNonemptyObjectsStreamProcessorLoadStateFailure) {
  JsonSelectNonemptyObjectsStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  {
    JsonSelectNonemptyObjectsProto proto;
    proto.set_state(4);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunSelectNonemptyArrays(std::string input) {
  return RunProcessor<JsonSelectNonemptyArraysStreamProcessor>(input);
}

TEST(JsonStreamTest, SelectNonemptyArraysStreamProcessor) {
  EXPECT_EQ(RunSelectNonemptyArrays(""), "");
  EXPECT_EQ(RunSelectNonemptyArrays("123\nnull\n{}\n[\"x\"]\n{\"a\":\"b\"}\n[]"
                                    "\n[1,2,3]\n{\"c\":\"d\"}\n"),
            "[\"x\"]\n[1,2,3]\n");
}

TEST(JsonStreamTest, SelectNonemptyArraysStreamProcessorLoadStateFailure) {
  JsonSelectNonemptyArraysStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  {
    JsonSelectNonemptyArraysProto proto;
    proto.set_state(4);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunSelectNonnull(std::string input) {
  return RunProcessor<JsonSelectNonnullStreamProcessor>(input);
}

TEST(JsonStreamTest, SelectNonnullStreamProcessor) {
  EXPECT_EQ(RunSelectNonnull(""), "");
  EXPECT_EQ(
      RunSelectNonnull(
          "123\nnull\n{}\n{\"a\":\"b\"}\n[]\nnull\n[1,2,3]\n{\"c\":\"d\"}\n"),
      "123\n{}\n{\"a\":\"b\"}\n[]\n[1,2,3]\n{\"c\":\"d\"}\n");
}

TEST(JsonStreamTest, SelectNonnullStreamProcessorLoadStateFailure) {
  JsonSelectNonnullStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  {
    JsonSelectNonnullProto proto;
    proto.set_state(4);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunExtractValues(
    std::string input,
    std::function<bool(absl::Span<const std::variant<int64_t, std::string>>)>
        path_match_fn,
    bool with_path = false) {
  return RunProcessor<JsonExtractValuesStreamProcessor,
                      JsonExtractValuesOptions>(
      input, JsonExtractValuesOptions{.path_match_fn = path_match_fn,
                                      .with_path = with_path});
}

TEST(JsonStreamTest, ExtractValuesStreamProcessor) {
  auto match_all = [](absl::Span<const std::variant<int64_t, std::string>>) {
    return true;
  };
  auto match_none = [](absl::Span<const std::variant<int64_t, std::string>>) {
    return false;
  };
  auto match_depth_1 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1;
      };
  auto match_key = [](std::string key) {
    return [key](absl::Span<const std::variant<int64_t, std::string>> path) {
      return path.size() == 1 && std::holds_alternative<std::string>(path[0]) &&
             std::get<std::string>(path[0]) == key;
    };
  };

  EXPECT_EQ(RunExtractValues("", match_all), "");
  EXPECT_EQ(RunExtractValues("", match_none), "");

  EXPECT_EQ(RunExtractValues("[1,2,3]\n", match_all), "[[1,2,3]]\n");
  EXPECT_EQ(RunExtractValues("[1,2,3]\n", match_none), "[]\n");
  EXPECT_EQ(RunExtractValues("[]\n", match_depth_1), "[]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":1}\n", match_depth_1), "[1]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1}\n", match_none), "[]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2}\n", match_depth_1), "[1,2]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2}\n", match_key("a")), "[1]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2}\n", match_key("b")), "[2]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2}\n", match_key("c")), "[]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":\"a\",\"a\":\"a\"}\n", match_depth_1),
            "[\"a\",\"a\"]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2,\"a\":3}\n", match_key("a")),
            "[1,3]\n");

  EXPECT_EQ(RunExtractValues("{\"😊\":1,\"\\ud83d\\ude0a\":2,\"x\":3}\n",
                             match_key("😊")),
            "[1,2]\n");

  EXPECT_EQ(RunExtractValues("[\"abc\"]\n", match_depth_1), "[\"abc\"]\n");
  EXPECT_EQ(RunExtractValues("[\"abc\\\\def\\\"ghi\"]\n", match_depth_1),
            "[\"abc\\\\def\\\"ghi\"]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":[1,2]}\n", match_depth_1), "[[1,2]]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":{\"b\":3}}\n", match_depth_1),
            "[{\"b\":3}]\n");

  EXPECT_EQ(RunExtractValues("[10,20,30]\n[40,50]\n", match_depth_1),
            "[10,20,30]\n[40,50]\n");

  EXPECT_EQ(RunExtractValues("null\n", match_none), "[]\n");
  EXPECT_EQ(RunExtractValues("null\n", match_all), "[null]\n");
  EXPECT_EQ(RunExtractValues("123\n", match_none), "[]\n");
  EXPECT_EQ(RunExtractValues("123\n", match_all), "[123]\n");

  EXPECT_EQ(RunExtractValues("true\n", match_all), "[true]\n");
  EXPECT_EQ(RunExtractValues("false\n", match_all), "[false]\n");
  EXPECT_EQ(RunExtractValues("\"hello\"\n", match_all), "[\"hello\"]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":[1,[2,3]],\"b\":4}\n", match_depth_1),
            "[[1,[2,3]],4]\n");

  EXPECT_EQ(
      RunExtractValues("{\"a\":{\"x\":1,\"y\":2},\"b\":3}\n", match_depth_1),
      "[{\"x\":1,\"y\":2},3]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":{\"x\":[1,2]},\"b\":[3,{\"y\":4}]}\n",
                             match_depth_1),
            "[{\"x\":[1,2]},[3,{\"y\":4}]]\n");

  EXPECT_EQ(
      RunExtractValues("{\"a\\\"b\":1}\n", match_depth_1), "[1]\n");
  auto match_escaped_key =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1 &&
               std::holds_alternative<std::string>(path[0]) &&
               std::get<std::string>(path[0]) == "a\"b";
      };
  EXPECT_EQ(RunExtractValues("{\"a\\\"b\":1}\n", match_escaped_key), "[1]\n");

  EXPECT_EQ(RunExtractValues("{\"a\\\\b\":1}\n", match_depth_1), "[1]\n");

  EXPECT_EQ(RunExtractValues("[10,20,30]\n", match_depth_1), "[10,20,30]\n");
  EXPECT_EQ(RunExtractValues("[\"a\",\"b\"]\n", match_depth_1),
            "[\"a\",\"b\"]\n");
  EXPECT_EQ(RunExtractValues("[[1,2],[3,4]]\n", match_depth_1),
            "[[1,2],[3,4]]\n");
  EXPECT_EQ(RunExtractValues("[{\"x\":1},{\"y\":2}]\n", match_depth_1),
            "[{\"x\":1},{\"y\":2}]\n");

  EXPECT_EQ(RunExtractValues("[\"a\\\"b\"]\n", match_depth_1),
            "[\"a\\\"b\"]\n");
  EXPECT_EQ(RunExtractValues("[\"a\\\\b\"]\n", match_depth_1),
            "[\"a\\\\b\"]\n");

  EXPECT_EQ(RunExtractValues("123\n456\n789\n", match_all),
            "[123]\n[456]\n[789]\n");
  EXPECT_EQ(
      RunExtractValues("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n", match_depth_1),
      "[1]\n[2]\n[3]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1}\n{\"b\":2}\n", match_key("a")),
            "[1]\n[]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1}\n{\"b\":2}\n", match_key("b")),
            "[]\n[2]\n");

  EXPECT_EQ(RunExtractValues("[]\n", match_depth_1), "[]\n");
  EXPECT_EQ(RunExtractValues("{}\n", match_depth_1), "[]\n");
  EXPECT_EQ(RunExtractValues("[]\n", match_all), "[[]]\n");
  EXPECT_EQ(RunExtractValues("{}\n", match_all), "[{}]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2,\"c\":3}\n", match_depth_1),
            "[1,2,3]\n");

  auto match_index_0 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1 &&
               std::holds_alternative<int64_t>(path[0]) &&
               std::get<int64_t>(path[0]) == 0;
      };
  EXPECT_EQ(RunExtractValues("[10,20,30]\n", match_index_0), "[10]\n");
  EXPECT_EQ(RunExtractValues("[[1,2],20,30]\n", match_index_0), "[[1,2]]\n");
  EXPECT_EQ(RunExtractValues("[{\"x\":1},20]\n", match_index_0),
            "[{\"x\":1}]\n");

  auto match_index_1 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1 &&
               std::holds_alternative<int64_t>(path[0]) &&
               std::get<int64_t>(path[0]) == 1;
      };
  EXPECT_EQ(RunExtractValues("[10,20,30]\n", match_index_1), "[20]\n");
  EXPECT_EQ(RunExtractValues("[10,[20,21],30]\n", match_index_1),
            "[[20,21]]\n");
  EXPECT_EQ(RunExtractValues("[10,{\"a\":20},30]\n", match_index_1),
            "[{\"a\":20}]\n");

  auto match_index_2 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1 &&
               std::holds_alternative<int64_t>(path[0]) &&
               std::get<int64_t>(path[0]) == 2;
      };
  EXPECT_EQ(RunExtractValues("[10,20,30]\n", match_index_2), "[30]\n");
  EXPECT_EQ(RunExtractValues("[10,20,[30,31]]\n", match_index_2),
            "[[30,31]]\n");
  EXPECT_EQ(RunExtractValues("[10,20,{\"a\":30}]\n", match_index_2),
            "[{\"a\":30}]\n");
  EXPECT_EQ(RunExtractValues("[10,20]\n", match_index_2), "[]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":\"x\\\"y\"}\n", match_depth_1),
            "[\"x\\\"y\"]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":\"x\\\\y\"}\n", match_depth_1),
            "[\"x\\\\y\"]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":null,\"b\":true,\"c\":false}\n",
                             match_depth_1),
            "[null,true,false]\n");

  EXPECT_EQ(RunExtractValues("\"hello\"\n", match_none), "[]\n");
  EXPECT_EQ(RunExtractValues("true\n", match_none), "[]\n");
  EXPECT_EQ(RunExtractValues("false\n", match_none), "[]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":[[1,2],[3,4]]}\n", match_depth_1),
            "[[[1,2],[3,4]]]\n");
  EXPECT_EQ(
      RunExtractValues("{\"a\":{\"x\":{\"y\":1}}}\n", match_depth_1),
      "[{\"x\":{\"y\":1}}]\n");

  EXPECT_EQ(
      RunExtractValues(
          "{\"a\":[{\"x\":1},{\"y\":2}],\"b\":{\"c\":[3,4]}}\n",
          match_depth_1),
      "[[{\"x\":1},{\"y\":2}],{\"c\":[3,4]}]\n");

  EXPECT_EQ(RunExtractValues("[\"a\",null,true,false,123]\n", match_depth_1),
            "[\"a\",null,true,false,123]\n");

  EXPECT_EQ(RunExtractValues("null\n123\n\"abc\"\ntrue\n", match_all),
            "[null]\n[123]\n[\"abc\"]\n[true]\n");

  auto match_depth_0 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.empty();
      };
  EXPECT_EQ(RunExtractValues("[1,2,3]\n", match_depth_0), "[[1,2,3]]\n");
  EXPECT_EQ(RunExtractValues("{\"a\":1}\n", match_depth_0), "[{\"a\":1}]\n");
  EXPECT_EQ(RunExtractValues("null\n", match_depth_0), "[null]\n");
  EXPECT_EQ(RunExtractValues("123\n", match_depth_0), "[123]\n");
  EXPECT_EQ(RunExtractValues("\"str\"\n", match_depth_0), "[\"str\"]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":[],\"b\":{}}\n", match_depth_1),
            "[[],{}]\n");

  EXPECT_EQ(RunExtractValues("[1]\n\n[2]\n", match_depth_1),
            "[1]\n[]\n[2]\n");
}

TEST(JsonStreamTest, ExtractValuesStreamProcessorWithPath) {
  auto match_all = [](absl::Span<const std::variant<int64_t, std::string>>) {
    return true;
  };
  auto match_depth_1 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1;
      };
  auto match_depth_2 =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 2;
      };

  EXPECT_EQ(RunExtractValues("", match_all, true), "");
  EXPECT_EQ(RunExtractValues("[1,2,3]\n", match_depth_1, true),
            "[[[0],1],[[1],2],[[2],3]]\n");
  EXPECT_EQ(
      RunExtractValues("{\"a\":1,\"b\":2,\"a\":3}\n", match_depth_1, true),
      "[[[\"a\"],1],[[\"b\"],2],[[\"a\"],3]]\n");

  EXPECT_EQ(RunExtractValues("[[1],{\"x\":2},[[3]]]\n", match_depth_2, true),
            "[[[0,0],1],[[1,\"x\"],2],[[2,0],[3]]]\n");
  EXPECT_EQ(RunExtractValues("[[]]\n", match_depth_2, true), "[]\n");

  EXPECT_EQ(RunExtractValues("123\n456\n789\n", match_all, true),
            "[[[],123]]\n[[[],456]]\n[[[],789]]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":1,\"b\":2}\n", match_depth_1, true),
            "[[[\"a\"],1],[[\"b\"],2]]\n");

  EXPECT_EQ(
      RunExtractValues("{\"a\":[1,2],\"b\":{\"c\":3}}\n", match_depth_1, true),
      "[[[\"a\"],[1,2]],[[\"b\"],{\"c\":3}]]\n");

  EXPECT_EQ(RunExtractValues("[10,20,30]\n[40]\n", match_depth_1, true),
            "[[[0],10],[[1],20],[[2],30]]\n[[[0],40]]\n");

  EXPECT_EQ(RunExtractValues("{}\n", match_depth_1, true), "[]\n");
  EXPECT_EQ(RunExtractValues("[]\n", match_depth_1, true), "[]\n");

  EXPECT_EQ(RunExtractValues("null\n", match_all, true), "[[[],null]]\n");
  EXPECT_EQ(RunExtractValues("\"abc\"\n", match_all, true),
            "[[[],\"abc\"]]\n");
  EXPECT_EQ(RunExtractValues("true\n", match_all, true), "[[[],true]]\n");
  EXPECT_EQ(RunExtractValues("false\n", match_all, true), "[[[],false]]\n");

  EXPECT_EQ(RunExtractValues("{\"a\":1}\n{\"b\":2}\n", match_depth_1, true),
            "[[[\"a\"],1]]\n[[[\"b\"],2]]\n");

  auto match_key_with_path = [](std::string key) {
    return [key](absl::Span<const std::variant<int64_t, std::string>> path) {
      return path.size() == 1 &&
             std::holds_alternative<std::string>(path[0]) &&
             std::get<std::string>(path[0]) == key;
    };
  };
  EXPECT_EQ(
      RunExtractValues("{\"a\":1,\"b\":2}\n", match_key_with_path("a"), true),
      "[[[\"a\"],1]]\n");
  EXPECT_EQ(
      RunExtractValues("{\"a\":1,\"b\":2}\n", match_key_with_path("b"), true),
      "[[[\"b\"],2]]\n");

  auto match_index_0_with_path =
      [](absl::Span<const std::variant<int64_t, std::string>> path) {
        return path.size() == 1 &&
               std::holds_alternative<int64_t>(path[0]) &&
               std::get<int64_t>(path[0]) == 0;
      };
  EXPECT_EQ(
      RunExtractValues("[10,20,30]\n", match_index_0_with_path, true),
      "[[[0],10]]\n");
  EXPECT_EQ(
      RunExtractValues("[[1,2],20]\n", match_index_0_with_path, true),
      "[[[0],[1,2]]]\n");
  EXPECT_EQ(
      RunExtractValues("[{\"x\":1},20]\n", match_index_0_with_path, true),
      "[[[0],{\"x\":1}]]\n");

  EXPECT_EQ(
      RunExtractValues(
          "{\"key\":{\"inner\":[1,2,3]}}\n", match_depth_2, true),
      "[[[\"key\",\"inner\"],[1,2,3]]]\n");
  EXPECT_EQ(RunExtractValues("[[10,20],[30]]\n", match_depth_2, true),
            "[[[0,0],10],[[0,1],20],[[1,0],30]]\n");

  EXPECT_EQ(RunExtractValues("null\n123\n\"x\"\n", match_all, true),
            "[[[],null]]\n[[[],123]]\n[[[],\"x\"]]\n");
}

TEST(JsonStreamTest, ExtractValuesStreamProcessorLoadStateFailure) {
  auto match_fn = [](absl::Span<const std::variant<int64_t, std::string>>) {
    return true;
  };
  JsonExtractValuesStreamProcessor processor(
      {.path_match_fn = match_fn, .with_path = false});

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  EXPECT_FALSE(
      processor.LoadState(JsonExtractValuesStreamProcessor(
                              {.path_match_fn = match_fn, .with_path = true})
                              .ToState()));
  {
    JsonExtractValuesStateProto proto;
    proto.set_with_path(false);
    proto.set_match_depth(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    JsonExtractValuesStateProto proto;
    proto.set_with_path(false);
    proto.add_container_path_stack();
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

std::string RunImplodeArray(std::string input) {
  return RunProcessor<JsonImplodeArrayStreamProcessor>(input);
}

TEST(JsonStreamTest, ImplodeArrayStreamProcessor) {
  EXPECT_EQ(RunImplodeArray(""), "[]\n");
  EXPECT_EQ(RunImplodeArray("123\n456\n789\n"), "[123,456,789]\n");
  EXPECT_EQ(RunImplodeArray("[]\n[[]]\n[[],[]]\n"), "[[],[[]],[[],[]]]\n");
  EXPECT_EQ(RunImplodeArray("\"abc]\\\\def\\\"[ghi\"\n"),
            "[\"abc]\\\\def\\\"[ghi\"]\n");
}

TEST(JsonStreamTest, ImplodeArrayStreamProcessorLoadStateFailure) {
  JsonImplodeArrayStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
}

TEST(JsonStreamTest, ImplodeArrayStreamProcessorMinimallyDelayed) {
  JsonImplodeArrayStreamProcessor processor;
  // 123\n"456"\n[789]\n"\\\""\n
  auto p = [&](std::string_view chunk) {
    return std::get<0>(processor.Process(chunk, false));
  };
  EXPECT_EQ(p("1"), "[1");
  EXPECT_EQ(p("2"), "2");
  EXPECT_EQ(p("3"), "3");
  EXPECT_EQ(p("\n"), "");
  EXPECT_EQ(p("\""), ",\"");
  EXPECT_EQ(p("4"), "4");
  EXPECT_EQ(p("5"), "5");
  EXPECT_EQ(p("6"), "6");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("\n"), "");
  EXPECT_EQ(p("["), ",[");
  EXPECT_EQ(p("7"), "7");
  EXPECT_EQ(p("8"), "8");
  EXPECT_EQ(p("9"), "9");
  EXPECT_EQ(p("]"), "]");
  EXPECT_EQ(p("\n"), "");
  EXPECT_EQ(p("\""), ",\"");
  EXPECT_EQ(p("\\"), "\\");
  EXPECT_EQ(p("\\"), "\\");
  EXPECT_EQ(p("\\"), "\\");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("\n"), "");
  EXPECT_EQ(std::get<0>(processor.Process("", true)), "]\n");
}

std::string RunExplodeArray(std::string input) {
  return RunProcessor<JsonExplodeArrayStreamProcessor>(input);
}

TEST(JsonStreamTest, ExplodeArrayStreamProcessor) {
  EXPECT_EQ(RunExplodeArray(""), "");
  EXPECT_EQ(RunExplodeArray("[]\n"), "");
  EXPECT_EQ(RunExplodeArray("[[]]\n"), "[]\n");
  EXPECT_EQ(RunExplodeArray("[[],{},[]]\n"), "[]\n{}\n[]\n");
  EXPECT_EQ(RunExplodeArray("[123,456,789]\n"), "123\n456\n789\n");
  EXPECT_EQ(RunExplodeArray("[123,456]\n[]\n[789]\n"), "123\n456\n789\n");
  EXPECT_EQ(RunExplodeArray("[\"abc\"]\n"), "\"abc\"\n");
  EXPECT_EQ(RunExplodeArray("[\"\\\\\\\"\"]\n"), "\"\\\\\\\"\"\n");
  EXPECT_EQ(RunExplodeArray("[\"abc\",\"\\\\\\\"\"]\n[]\n[\"def\"]\n"),
            "\"abc\"\n\"\\\\\\\"\"\n\"def\"\n");
}

TEST(JsonStreamTest, ExplodeArrayStreamProcessorLoadStateFailure) {
  JsonExplodeArrayStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  {
    JsonExplodeArrayStateProto proto;
    proto.set_container_depth(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

TEST(JsonStreamTest, ExplodeArrayStreamProcessorMinimallyDelayed) {
  JsonExplodeArrayStreamProcessor processor;
  // [123,"456",[789],"\\\""]
  auto p = [&](std::string_view chunk) {
    return std::get<0>(processor.Process(chunk, false));
  };
  EXPECT_EQ(p("["), "");
  EXPECT_EQ(p("1"), "1");
  EXPECT_EQ(p("2"), "2");
  EXPECT_EQ(p("3"), "3");
  EXPECT_EQ(p(","), "\n");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("4"), "4");
  EXPECT_EQ(p("5"), "5");
  EXPECT_EQ(p("6"), "6");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p(","), "\n");
  EXPECT_EQ(p("["), "[");
  EXPECT_EQ(p("7"), "7");
  EXPECT_EQ(p("8"), "8");
  EXPECT_EQ(p("9"), "9");
  EXPECT_EQ(p("]"), "]");
  EXPECT_EQ(p(","), "\n");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("\\"), "\\");
  EXPECT_EQ(p("\\"), "\\");
  EXPECT_EQ(p("\\"), "\\");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("\""), "\"");
  EXPECT_EQ(p("]"), "\n");
  EXPECT_EQ(std::get<0>(processor.Process("", true)), "");
}

std::string RunGetArrayNthValue(std::string input, int n) {
  return RunProcessor<JsonGetArrayNthValueStreamProcessor,
                      JsonGetArrayNthValueOptions>(
      input, JsonGetArrayNthValueOptions{.n = n});
}

TEST(JsonStreamTest, GetArrayNthValueStreamProcessor) {
  EXPECT_EQ(RunGetArrayNthValue("", 0), "");
  EXPECT_EQ(RunGetArrayNthValue("", 1), "");
  EXPECT_EQ(RunGetArrayNthValue("{}\n", 0), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("123\n", 0), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("\"abc\"\n", 0), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("\"[]\"\n", 0), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[]\n", 0), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[]\n", 1), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[123]\n", 0), "123\n");
  EXPECT_EQ(RunGetArrayNthValue("[123]\n", 1), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[\"abc\"]\n", 0), "\"abc\"\n");
  EXPECT_EQ(RunGetArrayNthValue("[\"abc\"]\n", 1), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[\"]\\\\\\\"\"]\n", 0), "\"]\\\\\\\"\"\n");
  EXPECT_EQ(RunGetArrayNthValue("[\"]\\\\\\\"\"]\n", 1), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[{}]\n", 0), "{}\n");
  EXPECT_EQ(RunGetArrayNthValue("[{}]\n", 1), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[123,456,789]\n", 0), "123\n");
  EXPECT_EQ(RunGetArrayNthValue("[123,456,789]\n", 1), "456\n");
  EXPECT_EQ(RunGetArrayNthValue("[123,456,789]\n", 2), "789\n");
  EXPECT_EQ(RunGetArrayNthValue("[123,456,789]\n", 3), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[[],[[]],[[],[]]]\n", 0), "[]\n");
  EXPECT_EQ(RunGetArrayNthValue("[[],[[]],[[],[]]]\n", 1), "[[]]\n");
  EXPECT_EQ(RunGetArrayNthValue("[[],[[]],[[],[]]]\n", 2), "[[],[]]\n");
  EXPECT_EQ(RunGetArrayNthValue("[[],[[]],[[],[]]]\n", 3), "null\n");
  EXPECT_EQ(RunGetArrayNthValue("[1,2,3]\n[4]\n[5,6]\n", 0), "1\n4\n5\n");
  EXPECT_EQ(RunGetArrayNthValue("[1,2,3]\n[4]\n[5,6]\n", 1), "2\nnull\n6\n");
  EXPECT_EQ(RunGetArrayNthValue("[1,2,3]\n[4]\n[5,6]\n", 2), "3\nnull\nnull\n");
  EXPECT_EQ(RunGetArrayNthValue("[1,2,3]\n[4]\n[5,6]\n", 3),
            "null\nnull\nnull\n");
}

TEST(JsonStreamTest, GetArrayNthValueStreamProcessorLoadStateFailure) {
  JsonGetArrayNthValueStreamProcessor processor({.n = 0});

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  {
    JsonGetArrayNthValueStateProto proto;
    proto.set_n(0);
    proto.set_container_depth(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  EXPECT_FALSE(processor.LoadState(
      JsonGetArrayNthValueStreamProcessor({.n = 1}).ToState()));
}

TEST(JsonStreamTest, GetArrayNthValueStreamProcessorMinimallyDelayed) {
  JsonGetArrayNthValueStreamProcessor processor({.n = 2});
  // [123,"456",[789],"\\\""]
  auto p = [&](std::string_view chunk) {
    return std::get<0>(processor.Process(chunk, false));
  };
  EXPECT_EQ(p("["), "");
  EXPECT_EQ(p("1"), "");
  EXPECT_EQ(p("2"), "");
  EXPECT_EQ(p("3"), "");
  EXPECT_EQ(p(","), "");
  EXPECT_EQ(p("\""), "");
  EXPECT_EQ(p("4"), "");
  EXPECT_EQ(p("5"), "");
  EXPECT_EQ(p("6"), "");
  EXPECT_EQ(p("\""), "");
  EXPECT_EQ(p(","), "");
  EXPECT_EQ(p("["), "[");
  EXPECT_EQ(p("7"), "7");
  EXPECT_EQ(p("8"), "8");
  EXPECT_EQ(p("9"), "9");
  EXPECT_EQ(p("]"), "]");
  EXPECT_EQ(p(","), "\n");
  EXPECT_EQ(p("\""), "");
  EXPECT_EQ(p("\\"), "");
  EXPECT_EQ(p("\\"), "");
  EXPECT_EQ(p("\\"), "");
  EXPECT_EQ(p("\""), "");
  EXPECT_EQ(p("\""), "");
  EXPECT_EQ(p("]"), "");
  EXPECT_EQ(std::get<0>(processor.Process("", true)), "");
}

std::string RunUnquote(std::string input) {
  return RunProcessor<JsonUnquoteStreamProcessor>(input);
}

TEST(JsonStreamTest, UnquoteStreamProcessor) {
  // Zero to many top-level inputs.
  EXPECT_EQ(RunUnquote(""), "");
  EXPECT_EQ(RunUnquote("\"abc\""), "abc");
  EXPECT_EQ(RunUnquote("\"abc\" \"def\" \"ghi\""), "abcdefghi");

  // Escapes.
  EXPECT_EQ(RunUnquote("\"\\\\\""), "\\");
  EXPECT_EQ(RunUnquote("\"\\\"\""), "\"");
  EXPECT_EQ(RunUnquote("\"\\/\""), "/");
  EXPECT_EQ(RunUnquote("\"\\b\""), "\b");
  EXPECT_EQ(RunUnquote("\"\\f\""), "\f");
  EXPECT_EQ(RunUnquote("\"\\n\""), "\n");
  EXPECT_EQ(RunUnquote("\"\\r\""), "\r");
  EXPECT_EQ(RunUnquote("\"\\t\""), "\t");
  {
    std::string null_string;
    null_string.push_back('\x00');
    EXPECT_EQ(RunUnquote("\"\\u0000\""), null_string);
  }
  EXPECT_EQ(RunUnquote("\"\\u001f\""), "\x1f");
  EXPECT_EQ(RunUnquote("\"\\u0042\""), "B");
  EXPECT_EQ(RunUnquote("\"\\u2660\""), "♠");
  EXPECT_EQ(RunUnquote("\"\\ud83d\\ude0a\""), "😊");

  // Non-ascii data is preserved.
  EXPECT_EQ(RunUnquote("\"♠\""), "♠");
  EXPECT_EQ(RunUnquote("\"😊\""), "😊");

  // Non-string values (behavior is implementation-defined).
  EXPECT_EQ(RunUnquote("[[], {\"x\": 123}]"), "x");

  // Invalid UTF-16 surrogate pair escapes (behavior is implementation-defined).
  EXPECT_EQ(RunUnquote("\"\\ud83d\""), "\ufffd");
  EXPECT_EQ(RunUnquote("\"\\ude0a\""), "\ufffd");
  EXPECT_EQ(RunUnquote("\"\\ude0a\\ud83d\""), "\ufffd\ufffd");
  EXPECT_EQ(RunUnquote("\"\\ud83d\\u1234\""), "\ufffd");

  // Truncated escapes (behavior is implementation-defined).
  EXPECT_EQ(RunUnquote("\"\\u123\""), "\ufffd");
  EXPECT_EQ(RunUnquote("\"\\u123\"\"abc\""), "\ufffdabc");
  EXPECT_EQ(RunUnquote("\"\\u123\\u456\""), "\ufffdu456");
  EXPECT_EQ(RunUnquote("\"?\\u123??\""), "?\ufffd?");
  EXPECT_EQ(RunUnquote("\"?\\u123u?\""), "?\ufffd?");
  EXPECT_EQ(RunUnquote("\"?\\u1uu4?\""), "?\ufffdu4?");
  EXPECT_EQ(RunUnquote("\"?\\ud83d\\u567u?\""), "?\ufffd?");
  EXPECT_EQ(RunUnquote("\"?\\ud83d\\udeua?\""), "?\ufffda?");
}

TEST(JsonStreamTest, UnquoteStreamProcessorLoadStateFailure) {
  JsonUnquoteStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
}

std::string RunQuote(std::string input) {
  return RunProcessor<JsonQuoteStreamProcessor>(input);
}

TEST(JsonStreamTest, QuoteStreamProcessor) {
  EXPECT_EQ(RunQuote(""), "\"\"");
  EXPECT_EQ(RunQuote("abc"), "\"abc\"");
  EXPECT_EQ(RunQuote("abc def ghi"), "\"abc def ghi\"");
  EXPECT_EQ(RunQuote("\""), "\"\\\"\"");
  EXPECT_EQ(RunQuote("\\"), "\"\\\\\"");
  {
    std::string null_string;
    null_string.push_back('\x00');
    EXPECT_EQ(RunQuote(null_string), "\"\\u0000\"");
  }
  EXPECT_EQ(RunQuote("\x1f"), "\"\\u001f\"");
  EXPECT_EQ(RunQuote("\x20"), "\" \"");
  EXPECT_EQ(RunQuote("\b"), "\"\\b\"");
  EXPECT_EQ(RunQuote("\f"), "\"\\f\"");
  EXPECT_EQ(RunQuote("\n"), "\"\\n\"");
  EXPECT_EQ(RunQuote("\r"), "\"\\r\"");
  EXPECT_EQ(RunQuote("\t"), "\"\\t\"");
  EXPECT_EQ(RunQuote("♠"), "\"♠\"");
  EXPECT_EQ(RunQuote("😊"), "\"😊\"");
  EXPECT_EQ(RunQuote("\\u2660"), "\"\\\\u2660\"");
}

TEST(JsonStreamTest, QuoteStreamProcessorLoadStateFailure) {
  JsonQuoteStreamProcessor processor;

  EXPECT_FALSE(processor.LoadState("GARBAGE"));
}

}  // namespace
}  // namespace koladata::internal
