MBLENGTH = {
        8:1,
        33:3,
        88:2,
        91:2
        }

class Charset:
    def __init__(self, id, name, collation, is_default):
        self.id, self.name, self.collation = id, name, collation
        self.is_default = is_default == 'Yes'

class Charsets:
    def __init__(self):
        self._by_id = {}

    def add(self, c):
        self._by_id[c.id] = c

    def by_id(self, id):
        return self._by_id[id]

    def by_name(self, name):
        for c in self._by_id.values():
            if c.name == name and c.is_default:
                return c

_charsets = Charsets()
"""
Generated with:

mysql -N -s -e "select id, character_set_name, collation_name, is_default
from information_schema.collations order by id;" | python -c "import sys
for l in sys.stdin.readlines():
        id, name, collation, is_default  = l.split(chr(9))
        print '_charsets.add(Charset(%s, \'%s\', \'%s\', \'%s\'))' \
                % (id, name, collation, is_default.strip())
"

"""
_charsets.add(Charset(1, 'big5', 'big5_chinese_ci', 'Yes'))
_charsets.add(Charset(2, 'latin2', 'latin2_czech_cs', ''))
_charsets.add(Charset(3, 'dec8', 'dec8_swedish_ci', 'Yes'))
_charsets.add(Charset(4, 'cp850', 'cp850_general_ci', 'Yes'))
_charsets.add(Charset(5, 'latin1', 'latin1_german1_ci', ''))
_charsets.add(Charset(6, 'hp8', 'hp8_english_ci', 'Yes'))
_charsets.add(Charset(7, 'koi8r', 'koi8r_general_ci', 'Yes'))
_charsets.add(Charset(8, 'latin1', 'latin1_swedish_ci', 'Yes'))
_charsets.add(Charset(9, 'latin2', 'latin2_general_ci', 'Yes'))
_charsets.add(Charset(10, 'swe7', 'swe7_swedish_ci', 'Yes'))
_charsets.add(Charset(11, 'ascii', 'ascii_general_ci', 'Yes'))
_charsets.add(Charset(12, 'ujis', 'ujis_japanese_ci', 'Yes'))
_charsets.add(Charset(13, 'sjis', 'sjis_japanese_ci', 'Yes'))
_charsets.add(Charset(14, 'cp1251', 'cp1251_bulgarian_ci', ''))
_charsets.add(Charset(15, 'latin1', 'latin1_danish_ci', ''))
_charsets.add(Charset(16, 'hebrew', 'hebrew_general_ci', 'Yes'))
_charsets.add(Charset(18, 'tis620', 'tis620_thai_ci', 'Yes'))
_charsets.add(Charset(19, 'euckr', 'euckr_korean_ci', 'Yes'))
_charsets.add(Charset(20, 'latin7', 'latin7_estonian_cs', ''))
_charsets.add(Charset(21, 'latin2', 'latin2_hungarian_ci', ''))
_charsets.add(Charset(22, 'koi8u', 'koi8u_general_ci', 'Yes'))
_charsets.add(Charset(23, 'cp1251', 'cp1251_ukrainian_ci', ''))
_charsets.add(Charset(24, 'gb2312', 'gb2312_chinese_ci', 'Yes'))
_charsets.add(Charset(25, 'greek', 'greek_general_ci', 'Yes'))
_charsets.add(Charset(26, 'cp1250', 'cp1250_general_ci', 'Yes'))
_charsets.add(Charset(27, 'latin2', 'latin2_croatian_ci', ''))
_charsets.add(Charset(28, 'gbk', 'gbk_chinese_ci', 'Yes'))
_charsets.add(Charset(29, 'cp1257', 'cp1257_lithuanian_ci', ''))
_charsets.add(Charset(30, 'latin5', 'latin5_turkish_ci', 'Yes'))
_charsets.add(Charset(31, 'latin1', 'latin1_german2_ci', ''))
_charsets.add(Charset(32, 'armscii8', 'armscii8_general_ci', 'Yes'))
_charsets.add(Charset(33, 'utf8', 'utf8_general_ci', 'Yes'))
_charsets.add(Charset(34, 'cp1250', 'cp1250_czech_cs', ''))
_charsets.add(Charset(35, 'ucs2', 'ucs2_general_ci', 'Yes'))
_charsets.add(Charset(36, 'cp866', 'cp866_general_ci', 'Yes'))
_charsets.add(Charset(37, 'keybcs2', 'keybcs2_general_ci', 'Yes'))
_charsets.add(Charset(38, 'macce', 'macce_general_ci', 'Yes'))
_charsets.add(Charset(39, 'macroman', 'macroman_general_ci', 'Yes'))
_charsets.add(Charset(40, 'cp852', 'cp852_general_ci', 'Yes'))
_charsets.add(Charset(41, 'latin7', 'latin7_general_ci', 'Yes'))
_charsets.add(Charset(42, 'latin7', 'latin7_general_cs', ''))
_charsets.add(Charset(43, 'macce', 'macce_bin', ''))
_charsets.add(Charset(44, 'cp1250', 'cp1250_croatian_ci', ''))
_charsets.add(Charset(47, 'latin1', 'latin1_bin', ''))
_charsets.add(Charset(48, 'latin1', 'latin1_general_ci', ''))
_charsets.add(Charset(49, 'latin1', 'latin1_general_cs', ''))
_charsets.add(Charset(50, 'cp1251', 'cp1251_bin', ''))
_charsets.add(Charset(51, 'cp1251', 'cp1251_general_ci', 'Yes'))
_charsets.add(Charset(52, 'cp1251', 'cp1251_general_cs', ''))
_charsets.add(Charset(53, 'macroman', 'macroman_bin', ''))
_charsets.add(Charset(57, 'cp1256', 'cp1256_general_ci', 'Yes'))
_charsets.add(Charset(58, 'cp1257', 'cp1257_bin', ''))
_charsets.add(Charset(59, 'cp1257', 'cp1257_general_ci', 'Yes'))
_charsets.add(Charset(63, 'binary', 'binary', 'Yes'))
_charsets.add(Charset(64, 'armscii8', 'armscii8_bin', ''))
_charsets.add(Charset(65, 'ascii', 'ascii_bin', ''))
_charsets.add(Charset(66, 'cp1250', 'cp1250_bin', ''))
_charsets.add(Charset(67, 'cp1256', 'cp1256_bin', ''))
_charsets.add(Charset(68, 'cp866', 'cp866_bin', ''))
_charsets.add(Charset(69, 'dec8', 'dec8_bin', ''))
_charsets.add(Charset(70, 'greek', 'greek_bin', ''))
_charsets.add(Charset(71, 'hebrew', 'hebrew_bin', ''))
_charsets.add(Charset(72, 'hp8', 'hp8_bin', ''))
_charsets.add(Charset(73, 'keybcs2', 'keybcs2_bin', ''))
_charsets.add(Charset(74, 'koi8r', 'koi8r_bin', ''))
_charsets.add(Charset(75, 'koi8u', 'koi8u_bin', ''))
_charsets.add(Charset(77, 'latin2', 'latin2_bin', ''))
_charsets.add(Charset(78, 'latin5', 'latin5_bin', ''))
_charsets.add(Charset(79, 'latin7', 'latin7_bin', ''))
_charsets.add(Charset(80, 'cp850', 'cp850_bin', ''))
_charsets.add(Charset(81, 'cp852', 'cp852_bin', ''))
_charsets.add(Charset(82, 'swe7', 'swe7_bin', ''))
_charsets.add(Charset(83, 'utf8', 'utf8_bin', ''))
_charsets.add(Charset(84, 'big5', 'big5_bin', ''))
_charsets.add(Charset(85, 'euckr', 'euckr_bin', ''))
_charsets.add(Charset(86, 'gb2312', 'gb2312_bin', ''))
_charsets.add(Charset(87, 'gbk', 'gbk_bin', ''))
_charsets.add(Charset(88, 'sjis', 'sjis_bin', ''))
_charsets.add(Charset(89, 'tis620', 'tis620_bin', ''))
_charsets.add(Charset(90, 'ucs2', 'ucs2_bin', ''))
_charsets.add(Charset(91, 'ujis', 'ujis_bin', ''))
_charsets.add(Charset(92, 'geostd8', 'geostd8_general_ci', 'Yes'))
_charsets.add(Charset(93, 'geostd8', 'geostd8_bin', ''))
_charsets.add(Charset(94, 'latin1', 'latin1_spanish_ci', ''))
_charsets.add(Charset(95, 'cp932', 'cp932_japanese_ci', 'Yes'))
_charsets.add(Charset(96, 'cp932', 'cp932_bin', ''))
_charsets.add(Charset(97, 'eucjpms', 'eucjpms_japanese_ci', 'Yes'))
_charsets.add(Charset(98, 'eucjpms', 'eucjpms_bin', ''))
_charsets.add(Charset(99, 'cp1250', 'cp1250_polish_ci', ''))
_charsets.add(Charset(128, 'ucs2', 'ucs2_unicode_ci', ''))
_charsets.add(Charset(129, 'ucs2', 'ucs2_icelandic_ci', ''))
_charsets.add(Charset(130, 'ucs2', 'ucs2_latvian_ci', ''))
_charsets.add(Charset(131, 'ucs2', 'ucs2_romanian_ci', ''))
_charsets.add(Charset(132, 'ucs2', 'ucs2_slovenian_ci', ''))
_charsets.add(Charset(133, 'ucs2', 'ucs2_polish_ci', ''))
_charsets.add(Charset(134, 'ucs2', 'ucs2_estonian_ci', ''))
_charsets.add(Charset(135, 'ucs2', 'ucs2_spanish_ci', ''))
_charsets.add(Charset(136, 'ucs2', 'ucs2_swedish_ci', ''))
_charsets.add(Charset(137, 'ucs2', 'ucs2_turkish_ci', ''))
_charsets.add(Charset(138, 'ucs2', 'ucs2_czech_ci', ''))
_charsets.add(Charset(139, 'ucs2', 'ucs2_danish_ci', ''))
_charsets.add(Charset(140, 'ucs2', 'ucs2_lithuanian_ci', ''))
_charsets.add(Charset(141, 'ucs2', 'ucs2_slovak_ci', ''))
_charsets.add(Charset(142, 'ucs2', 'ucs2_spanish2_ci', ''))
_charsets.add(Charset(143, 'ucs2', 'ucs2_roman_ci', ''))
_charsets.add(Charset(144, 'ucs2', 'ucs2_persian_ci', ''))
_charsets.add(Charset(145, 'ucs2', 'ucs2_esperanto_ci', ''))
_charsets.add(Charset(146, 'ucs2', 'ucs2_hungarian_ci', ''))
_charsets.add(Charset(192, 'utf8', 'utf8_unicode_ci', ''))
_charsets.add(Charset(193, 'utf8', 'utf8_icelandic_ci', ''))
_charsets.add(Charset(194, 'utf8', 'utf8_latvian_ci', ''))
_charsets.add(Charset(195, 'utf8', 'utf8_romanian_ci', ''))
_charsets.add(Charset(196, 'utf8', 'utf8_slovenian_ci', ''))
_charsets.add(Charset(197, 'utf8', 'utf8_polish_ci', ''))
_charsets.add(Charset(198, 'utf8', 'utf8_estonian_ci', ''))
_charsets.add(Charset(199, 'utf8', 'utf8_spanish_ci', ''))
_charsets.add(Charset(200, 'utf8', 'utf8_swedish_ci', ''))
_charsets.add(Charset(201, 'utf8', 'utf8_turkish_ci', ''))
_charsets.add(Charset(202, 'utf8', 'utf8_czech_ci', ''))
_charsets.add(Charset(203, 'utf8', 'utf8_danish_ci', ''))
_charsets.add(Charset(204, 'utf8', 'utf8_lithuanian_ci', ''))
_charsets.add(Charset(205, 'utf8', 'utf8_slovak_ci', ''))
_charsets.add(Charset(206, 'utf8', 'utf8_spanish2_ci', ''))
_charsets.add(Charset(207, 'utf8', 'utf8_roman_ci', ''))
_charsets.add(Charset(208, 'utf8', 'utf8_persian_ci', ''))
_charsets.add(Charset(209, 'utf8', 'utf8_esperanto_ci', ''))
_charsets.add(Charset(210, 'utf8', 'utf8_hungarian_ci', ''))

def charset_by_name(name):
    return _charsets.by_name(name)

def charset_by_id(id):
    return _charsets.by_id(id)

