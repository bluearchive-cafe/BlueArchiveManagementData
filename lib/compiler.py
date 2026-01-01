import os
import re
from enum import Enum
from lib.structure import EnumMember, EnumType, Property, StructTable
from lib.console import notice
from utils.util import TemplateString, Utils
from utils.config import Config

class DataSize(Enum):
    bool = 1; Boolean = 1
    byte = 1; Int8 = 1
    sbyte = 1
    ubyte = 1; Uint8 = 1
    short = 2; Int16 = 2
    ushort = 2; Uint16 = 2
    int = 4; Int32 = 4
    uint = 4; Uint32 = 4; UInt32 = 4
    long = 8; Int64 = 8
    ulong = 8; Uint64 = 8
    float = 4; Single = 4; Float32 = 4
    double = 8; Float64 = 8
    string = 4; String = 4
    struct = 4

class DataFlag(Enum):
    bool = "Bool"; Boolean = "Bool"
    byte = "Int8"; Int8 = "Int8"
    sbyte = "Int8"
    ubyte = "Uint8"; Uint8 = "Uint8"
    short = "Int16"; Int16 = "Int16"
    ushort = "Uint16"; Uint16 = "Uint16"
    int = "Int32"; Int32 = "Int32"
    uint = "Uint32"; Uint32 = "Uint32"; UInt32 = "Uint32"
    long = "Int64"; Int64 = "Int64"
    ulong = "Uint64"; Uint64 = "Uint64"
    float = "Float32"; Single = "Float32"; Float32 = "Float32"
    double = "Float64"; Float64 = "Float64"
    string = "String"; String = "String"

class ConvertFlag(Enum):
    bool = "bool"; Boolean = "bool"
    short = "convert_short"
    ushort = "convert_ushort"
    int = "convert_int"; Int32 = "convert_int"
    uint = "convert_uint"; UInt32 = "convert_uint"
    long = "convert_long"; Int64 = "convert_long"
    ulong = "convert_ulong"
    float = "convert_float"; Single = "convert_float"
    double = "convert_double"
    string = "convert_string"; String = "convert_string"

class String:
    INDENT = "    "
    NEWLINE = "\n"
    WRAPPER_WRAPPER_FUNC_CALL = TemplateString("dump_%s(%s)")
    WRAPPER_ENUM_NAME_CALL = TemplateString("%s.name")
    ENUM_CLASS = TemplateString("class %s:")
    VARIABLE_ASSIGNMENT = TemplateString("%s = %s")
    FUNCTION_DEFINE = TemplateString("def %s(%s)%s:")
    WRAPPER_BASE = """from enum import IntEnum
from lib.encryption import convert_short, convert_ushort, convert_int, convert_long, convert_float, convert_double, convert_string, convert_uint, convert_ulong, create_key
import inspect

def dump_table(table_instance) -> list:
    excel_name = table_instance.__class__.__name__.removesuffix("Table")
    current_module = inspect.getmodule(inspect.currentframe())
    dump_func = next(
        f
        for n, f in inspect.getmembers(current_module, inspect.isfunction)
        if n.removeprefix("dump_") == excel_name
    )
    password = create_key(excel_name.removesuffix("Excel"))
    return [dump_func(table_instance.DataList(j), password) for j in range(table_instance.DataListLength())]\n
"""
    CN_WRAPPER_BASE = """from enum import IntEnum
from lib.encryption import convert_short, convert_ushort, convert_int, convert_long, convert_float, convert_double, convert_string, convert_uint, convert_ulong, create_key
import inspect

def dump_table(table_instance) -> list:
    excel_name = table_instance.__class__.__name__.removesuffix("Table")
    current_module = inspect.getmodule(inspect.currentframe())
    dump_func = next(
        f
        for n, f in inspect.getmembers(current_module, inspect.isfunction)
        if n.removeprefix("dump_") == excel_name
    )
    return [dump_func(table_instance.DataList(j)) for j in range(table_instance.DataListLength())]\n
"""
    WRAPPER_GETTER = TemplateString("excel_instance.%s()")
    WRAPPER_LIST_GETTER = TemplateString("excel_instance.%s(j)")
    WRAPPER_LIST_CONVERTION = TemplateString(
        "%s for j in range(excel_instance.%sLength())"
    )
    WRAPPER_PASSWD_CONVERTION = TemplateString("%s(%s, password)")
    WRAPPER_ENUM_CONVERTION = TemplateString("%s(%s).name")
    WRAPPER_PROP_KV = TemplateString('"%s": %s,\n')
    WRAPPER_LIST_KV = TemplateString('"%s": [%s],\n')
    WRAPPER_FUNC = TemplateString(
        """
def dump_%s(excel_instance, password: bytes = b"") -> dict:
    return {\n%s    }
"""
    )
    WRAPPER_INT_ENUM = TemplateString("class %s(IntEnum):")
    LOCAL_IMPORT = TemplateString("from .%s import %s")
    FB_BASIC_CLASS = TemplateString(
        """
import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()\n
class %s:
    __slots__ = ['_tab']\n
    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = %s()
        x.Init(buf, n + offset)
        return x\n
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)\n
"""
    )
    FB_NON_SCALAR_LIST_CLASS_METHODS = TemplateString(
        """
    def %s(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            x = self._tab.Vector(o)
            x += flatbuffers.number_types.UOffsetTFlags.py_type(j) * %d
            x = self._tab.Indirect(x)
            from .%s import %s
            obj = %s()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None\n
    def %sLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0\n
    def %sIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        return o == 0\n
"""
    )
    FB_SCALAR_LIST_CLASS_METHODS = TemplateString(
        """
    def %s(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.%sFlags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * %d))
        return 0\n
    def %sAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.%sFlags, o)
        return 0\n
    def %sLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0\n
    def %sIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        return o == 0\n
"""
    )
    FB_SCALAR_PROPERTY_CLASS_METHODS = TemplateString(
        """
    def %s(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.%sFlags, o + self._tab.Pos)
        return 0\n
"""
    )

    FB_STRING_PROPERTY_CLASS_METHODS = TemplateString(
        """
    def %s(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None\n
"""
    )

    FB_STRING_LIST_CLASS_METHODS = TemplateString(
        """
    def %s(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return ""\n
    def %sLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0\n
    def %sIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        return o == 0\n
"""
    )
    FB_STRUCT_PROPERTY_CLASS_METHODS = TemplateString(
        """
    def %s(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            x = self._tab.Indirect(o + self._tab.Pos)
            from .%s import %s
            obj = %s()
            obj.Init(self._tab.Bytes, x)
            return obj
        return None\n
"""
    )
    FB_ISOLATED_PROPERTY_CLASS_METHODS = TemplateString(
        """
    def %s(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(%d))
        if o != 0:
            from .%s import %s
            obj = %s()
            obj.Init(self._tab.Bytes, o + self._tab.Pos)
            return obj
        return None\n
"""
    )
    FB_LIST_AND_NON_SCALAR_PROPERTY_FUNCTION = TemplateString(
        """
    @staticmethod
    def Add%s(builder, %s): builder.PrependUOffsetTRelativeSlot(%d, flatbuffers.number_types.UOffsetTFlags.py_type(%s), 0)
    @staticmethod
    def Start%sVector(builder, numElems): return builder.StartVector(%d, numElems, %d)\n
"""
    )
    FB_STRING_AND_STRUCT_PROPERTY_FUNCTION = TemplateString(
        """
    @staticmethod
    def Add%s(builder, %s): builder.PrependUOffsetTRelativeSlot(%d, flatbuffers.number_types.UOffsetTFlags.py_type(%s), 0)
"""
    )
    FB_SCALAR_PROPERTY_FUNCTION = TemplateString(
        """
    @staticmethod
    def Add%s(builder, %s): builder.Prepend%sSlot(%d, %s, 0)\n
"""
    )
    FB_START_AND_END_FUNCTION = TemplateString(
        """
    @staticmethod
    def Start(builder): builder.StartObject(%d)
    @staticmethod
    def End(builder): return builder.EndObject()\n
"""
    )

class Re:
    struct = re.compile(
        r"""\s*struct (.{0,128}?) :.{0,128}?IFlatbufferObject.{0,128}?
\s*\{
(.+?)
\s*\}
""",
        re.M | re.S,
    )
    struct_property = re.compile(r"""public (?:FlatData\.)?(.+?)\?? (.+?) { get(?: => default)?; }""")
    enum = re.compile(
        r"""
public\s+enum\s+(.{1,128}?)\s*//\s*TypeDefIndex:\s*\d+\s*
\{\s*
(.*?)
\s*\}
        """,
        re.M | re.S | re.X,
    )
    enum_member = re.compile(r"(.+?) = (-?\d+)")
    table_data_type = re.compile(r"public (.+?)\? DataList\(int j\) => default;")


class CNCSParser:
    def __init__(self, file_path: str) -> None:
        with open(file_path, "rt", encoding="utf8") as file:
            self.data = file.read()

    def parse_enum(self) -> list[EnumType]:
        enums = []
        enum_matches = re.finditer(
            r"public enum (.+?) :\s*// Fields\s*public (.+?)\s+value__;\s*(.+?)(?=\s*// Methods|\s*})",
            self.data,
            re.M | re.S
        )
        for match in enum_matches:
            enum_name = match.group(1).strip()
            enum_type = match.group(2).strip()
            content = match.group(3)
            if "." in enum_name:
                continue
            enum_type_mapping = {
                "System.Int32": "int",
                "System.Int16": "short",
                "System.Int64": "long",
                "System.Byte": "byte",
                "System.SByte": "sbyte",
                "System.UInt16": "ushort",
                "System.UInt32": "uint",
                "System.UInt64": "ulong"
            }
            actual_enum_type = enum_type_mapping.get(enum_type, "int")
            enum_members = []
            for line in content.split('\n'):
                member_match = re.match(r"\s*public const (.+?) (.+?) = ([^;]+);", line.strip())
                if member_match:
                    name = member_match.group(2).strip()
                    value = member_match.group(3).strip().rstrip(';')
                    enum_members.append(EnumMember(name, value))
            if enum_members:
                enums.append(EnumType(enum_name, actual_enum_type, enum_members))
        return enums

    def parse_struct(self) -> list[StructTable]:
        structs = []
        for match in re.finditer(
            r"// BlueArchive\.dll\s*public struct (.+?) : FlatBuffers\.IFlatbufferObject\s*(.+?)(\s*}\s*)",
            self.data,
            re.M | re.S
        ):
            full_struct_name = match.group(1).strip()
            struct_body = match.group(2)
            if not (full_struct_name.startswith("MX.Data.Excel") or full_struct_name.startswith("FlatData")):
                continue
            struct_name = full_struct_name.split('.')[-1]
            props = []
            seen_list_names = set()

            for list_match in re.finditer(r"public .+? (\w+)Length\(\);", struct_body):
                list_name = list_match.group(1)
                data_list_match = re.search(
                    rf"public System\.Nullable<(.+?)> {list_name}\(System\.Int32 j\);",
                    struct_body
                )
                if data_list_match:
                    list_type = data_list_match.group(1).replace("System.", "").replace("FlatData.", "")
                    props.append(Property(list_type, list_name, True))
                    seen_list_names.add(list_name)

            for prop_match in re.finditer(r"public (.+?) get_(.+?)\(\);", struct_body):
                p_type = prop_match.group(1).replace("System.", "").replace("FlatData.", "")
                p_name = prop_match.group(2)
                if p_name in ["ByteBuffer", "DataList"] or p_name.endswith("Length") or p_name in seen_list_names:
                    continue
                props.append(Property(p_type, p_name, False))

            if props:
                structs.append(StructTable(struct_name, props))

        final_structs = [s for s in structs if not s.name.endswith("ExcelTable")]
        for s in tuple(final_structs):
            if s.name.endswith("Excel"):
                final_structs.append(StructTable(s.name + "Table", [Property(s.name, 'DataList', True)]))
        return final_structs

class CSParser:
    def __init__(self, file_path: str) -> None:
        with open(file_path, "rt", encoding="utf8") as file:
            self.data = file.read()
            start_token = "namespace FlatData"
            start_idx = self.data.find(start_token)
            if start_idx == -1:
                self.flatdata_part = ""
                return
            brace_idx = self.data.find("{", start_idx)
            if brace_idx == -1:
                self.flatdata_part = ""
                return
            index = brace_idx
            open_braces = 1
            while index < len(self.data) - 1 and open_braces > 0:
                index += 1
                if self.data[index] == "{":
                    open_braces += 1
                elif self.data[index] == "}":
                    open_braces -= 1
            self.flatdata_part = self.data[start_idx:index + 1]

    def parse_enum(self) -> list[EnumType]:
        enums = []
        for enum_name, content in Re.enum.findall(self.flatdata_part):
            if "." in enum_name:
                continue
            enum_members = []
            for name, value in Re.enum_member.findall(content):
                enum_members.append(EnumMember(name.strip(), value))
            enums.append(EnumType(enum_name, "int", enum_members))
        return enums

    def __parse_struct_property(
        self, prop_type: str, prop_name: str, prop_data: str
    ) -> Property:
        prop_is_list = False
        prop_type = prop_type.removeprefix("Nullable<").removesuffix(">")
        if len(prop_name) > 6 and prop_name.endswith("Length"):
            list_name = prop_name.removesuffix("Length")
            re_type_of_list = re.search(
                rf"public (?:FlatData\.)?(.+?)\?? {list_name}\(int j\) => default;",
                prop_data
            )
            if re_type_of_list:
                list_type = re_type_of_list.group(1)
                prop_is_list = True
                list_type = list_type.removeprefix("Nullable<").removesuffix(">")
                return Property(list_type, list_name, prop_is_list)
        return Property(prop_type, prop_name, prop_is_list)

    def parse_struct(self) -> list[StructTable]:
        structs = []
        for struct_name, struct_data in Re.struct.findall(self.data):
            struct_properties = []
            for prop in Re.struct_property.finditer(struct_data):
                prop_type = prop.group(1)
                prop_name = prop.group(2)
                if "ByteBuffer" in prop_name:
                    continue
                if extracted_property := self.__parse_struct_property(
                    prop_type, prop_name, struct_data
                ):
                    struct_properties.append(extracted_property)
            if struct_properties:
                structs.append(StructTable(struct_name, struct_properties))
        structs = [struct for struct in structs if not struct.name.endswith("ExcelTable")]
        for struct in tuple(structs):
            if not struct.name.endswith("Excel"):
                continue
            structs.append(StructTable(struct.name + "Table", [Property(struct.name, 'DataList', True)]))
        return structs

class CompileToPython:
    DUMP_WRAPPER_NAME = "dump_wrapper"

    def __init__(self, enums: list[EnumType], structs: list[StructTable], extract_dir: str, encrypt: bool = True) -> None:
        self.enums = enums
        self.structs = structs
        self.extract_dir = extract_dir
        self.enums_by_name = {e.name: e for e in enums}
        self.structs_by_name = {s.name: s for s in structs}

    def _get_type_info(self, p_type):
        if p_type in self.enums_by_name: return self.enums_by_name[p_type]
        if p_type in self.structs_by_name: return self.structs_by_name[p_type]
        simple_name = p_type.split('.')[-1]
        if simple_name in self.enums_by_name: return self.enums_by_name[simple_name]
        if simple_name in self.structs_by_name: return self.structs_by_name[simple_name]
        return None

    def create_enum_files(self) -> None:
        os.makedirs(self.extract_dir, exist_ok=True)
        for enum in self.enums:
            name = Utils.convert_name_to_available(enum.name)
            with open(os.path.join(self.extract_dir, f"{name}.py"), "wt", encoding="utf8") as f:
                f.write(f"class {name}:\n")
                for m in enum.members:
                    f.write(f"    {Utils.convert_name_to_available(m.name)} = {m.value}\n")

    def create_struct_files(self) -> None:
        os.makedirs(self.extract_dir, exist_ok=True)
        for struct in self.structs:
            s_name = Utils.convert_name_to_available(struct.name)
            with open(os.path.join(self.extract_dir, f"{s_name}.py"), "wt", encoding="utf8") as f:
                f.write(String.FB_BASIC_CLASS(s_name, s_name))
                funcs = String.FB_START_AND_END_FUNCTION(len(struct.properties))
                
                for i, prop in enumerate(struct.properties):
                    offset = 4 + 2 * i
                    p_name = Utils.convert_name_to_available(prop.name)
                    p_type = prop.data_type
                    type_info = self._get_type_info(p_type)

                    is_string = p_type.lower().endswith("string")
                    is_enum = isinstance(type_info, EnumType)
                    lookup_key = type_info.underlying_type if is_enum else p_type
                    is_scalar = lookup_key in DataFlag.__members__

                    if is_string:
                        if prop.is_list:
                            f.write(String.FB_STRING_LIST_CLASS_METHODS(p_name, offset, p_name, offset, p_name, offset))
                            funcs += String.FB_LIST_AND_NON_SCALAR_PROPERTY_FUNCTION(p_name, p_name, i, p_name, p_name, 4, 4)
                        else:
                            f.write(String.FB_STRING_PROPERTY_CLASS_METHODS(p_name, offset))
                            funcs += String.FB_STRING_AND_STRUCT_PROPERTY_FUNCTION(p_name, p_name, i, p_name)

                    elif is_scalar:
                        flag = DataFlag[lookup_key].value
                        t_size = DataSize[lookup_key].value
                        if prop.is_list:
                            f.write(String.FB_SCALAR_LIST_CLASS_METHODS(p_name, offset, flag, t_size, p_name, offset, flag, p_name, offset, p_name, offset))
                            funcs += String.FB_LIST_AND_NON_SCALAR_PROPERTY_FUNCTION(p_name, p_name, i, p_name, p_name, t_size, t_size)
                        else:
                            f.write(String.FB_SCALAR_PROPERTY_CLASS_METHODS(p_name, offset, flag))
                            funcs += String.FB_SCALAR_PROPERTY_FUNCTION(p_name, p_name, flag, i, p_name)

                    elif isinstance(type_info, StructTable):
                        if prop.is_list:
                            f.write(String.FB_NON_SCALAR_LIST_CLASS_METHODS(p_name, offset, 4, p_type, p_type, p_type, p_name, offset, p_name, offset))
                            funcs += String.FB_LIST_AND_NON_SCALAR_PROPERTY_FUNCTION(p_name, p_name, i, p_name, p_name, 4, 4)
                        else:
                            f.write(String.FB_STRUCT_PROPERTY_CLASS_METHODS(p_name, offset, p_type, p_type, p_type))
                            funcs += String.FB_STRING_AND_STRUCT_PROPERTY_FUNCTION(p_name, p_name, i, p_name)

                    else:
                        f.write(String.FB_SCALAR_PROPERTY_CLASS_METHODS(p_name, offset, "Int32"))
                        funcs += String.FB_SCALAR_PROPERTY_FUNCTION(p_name, p_name, "Int32", i, p_name)

                f.write("\n\n" + funcs)

    def create_module_file(self) -> None:
        with open(os.path.join(self.extract_dir, "__init__.py"), "wt", encoding="utf8") as f:
            for e in self.enums: f.write(String.LOCAL_IMPORT(Utils.convert_name_to_available(e.name), Utils.convert_name_to_available(e.name)) + "\n")
            for s in self.structs: f.write(String.LOCAL_IMPORT(Utils.convert_name_to_available(s.name), Utils.convert_name_to_available(s.name)) + "\n")

    def create_dump_dict_file(self) -> None:
        with open(os.path.join(self.extract_dir, f"{self.DUMP_WRAPPER_NAME}.py"), "wt", encoding="utf8") as f:
            if Config.is_cn:
                f.write(String.CN_WRAPPER_BASE)
            else:
                f.write(String.WRAPPER_BASE)
            for e in self.enums:
                name = Utils.convert_name_to_available(e.name)
                f.write(f"class {name}(IntEnum):\n")
                for m in e.members: f.write(f"    {Utils.convert_name_to_available(m.name)} = {m.value}\n")
                f.write("\n")
            for s in self.structs:
                s_name, items = Utils.convert_name_to_available(s.name), ""
                for p in s.properties:
                    p_name, info, conv = Utils.convert_name_to_available(p.name), self._get_type_info(p.data_type), ""
                    getter = String.WRAPPER_LIST_GETTER(p_name) if p.is_list else String.WRAPPER_GETTER(p_name)
                    if p.data_type in ConvertFlag.__members__:
                        c_func = ConvertFlag[p.data_type].value
                        conv = f"bool({getter})" if c_func == "bool" else String.WRAPPER_PASSWD_CONVERTION(c_func, getter)
                    elif isinstance(info, EnumType):
                        u_type = info.underlying_type
                        u_func = ConvertFlag[u_type].value if u_type in ConvertFlag.__members__ else "convert_int"
                        inner = String.WRAPPER_PASSWD_CONVERTION(u_func, getter)
                        conv = String.WRAPPER_ENUM_CONVERTION(Utils.convert_name_to_available(info.name), inner)
                    elif isinstance(info, StructTable):
                        conv = String.WRAPPER_PASSWD_CONVERTION(f"dump_{Utils.convert_name_to_available(info.name)}", getter)
                    else:
                        conv = getter
                    items += "        " + (String.WRAPPER_LIST_KV(p_name, String.WRAPPER_LIST_CONVERTION(conv, p_name)) if p.is_list else String.WRAPPER_PROP_KV(p_name, conv))
                f.write(String.WRAPPER_FUNC(s_name, items))

    def create_repack_dict_file(self) -> None:
        WRAPPER_PACK_BASE = """import flatbuffers
from lib.encryption import xor, create_key, convert_short, convert_ushort, convert_int, convert_uint, convert_long, convert_ulong, encrypt_float, encrypt_double, encrypt_string
from . import *
    """
        self.enums_by_name = {enum.name: enum for enum in self.enums}
        self.structs_by_name = {struct.name : struct for struct in self.structs}
        os.makedirs(self.extract_dir, exist_ok=True)
        repack_path = os.path.join(self.extract_dir, "repack_wrapper.py")
        
        with open(repack_path, "wt", encoding="utf8") as file:
            file.write(WRAPPER_PACK_BASE)
            file.write("\n\n")

            for struct in self.structs:
                struct_name = Utils.convert_name_to_available(struct.name)
                if struct_name.endswith("ExcelTable"):
                    record_type = struct_name[:-5]
                    file.write(f"def pack_{struct_name}(builder: flatbuffers.Builder, dump_list: list, encrypt=True) -> int:\n")
                    file.write("    offsets = []\n")
                    file.write("    for record in dump_list:\n")
                    file.write(f"        offsets.append(pack_{record_type}(builder, record, encrypt))\n")
                    file.write(f"    {struct_name}.StartDataListVector(builder, len(offsets))\n")
                    file.write("    for offset in reversed(offsets):\n")
                    file.write("        builder.PrependUOffsetTRelative(offset)\n")
                    file.write("    data_list = builder.EndVector(len(offsets))\n")
                    file.write(f"    {struct_name}.Start(builder)\n")
                    file.write(f"    {struct_name}.AddDataList(builder, data_list)\n")
                    file.write(f"    return {struct_name}.End(builder)\n\n")
                    continue

                file.write(f"def pack_{struct_name}(builder: flatbuffers.Builder, data: dict, encrypt=True) -> int:\n")
                password_key = struct.name[:-5] if struct.name.endswith("Excel") else struct.name
                file.write(f'    password = create_key("{password_key}") if encrypt else None\n')
                
                # Process all strings first
                string_fields = [prop for prop in struct.properties if prop.data_type == "string" and not prop.is_list]
                for prop in string_fields:
                    file.write(f"    {prop.name}_off = builder.CreateString(encrypt_string(data.get('{prop.name}', ''), password))\n")

                # Process vectors with proper element handling
                vector_fields = [prop for prop in struct.properties if prop.is_list]
                for prop in vector_fields:
                    file.write(f"    {prop.name}_vec = 0\n")
                    file.write(f"    if '{prop.name}' in data:\n")
                    file.write(f"        {prop.name}_items = data['{prop.name}']\n")
                    elem, data_type = self._get_conversion_code(prop, "item")
                    if data_type == "string":
                        file.write(f"        {prop.name}_str_offsets = [builder.CreateString(encrypt_string(item, password)) for item in {prop.name}_items]\n")
                        file.write(f"        {struct_name}.Start{prop.name}Vector(builder, len({prop.name}_str_offsets))\n")
                        file.write(f"        for offset in reversed({prop.name}_str_offsets):\n")
                        file.write(f"            builder.PrependUOffsetTRelative(offset)\n")
                    elif data_type in self.structs_by_name:
                        elem = f"pack_{data_type}(builder, item, encrypt)"
                    else:
                        if data_type not in DataFlag.__members__:
                            print(data_type)
                        file.write(f"        {struct_name}.Start{prop.name}Vector(builder, len({prop.name}_items))\n")
                        file.write(f"        for item in reversed({prop.name}_items):\n")
                        file.write(f"            builder.Prepend{DataFlag.__members__.get(data_type, DataFlag.int).value}({elem})\n")
                    
                    file.write(f"        {prop.name}_vec = builder.EndVector(len({prop.name}_items))\n")

                # Process scalar values
                scalar_fields = [prop for prop in struct.properties if not prop.is_list and prop.data_type != "string"]
                for prop in scalar_fields:
                    conv_code, _ = self._get_conversion_code(prop, f"data.get('{prop.name}', 0)")
                    file.write(f"    {prop.name}_val = {conv_code}\n")

                # Build final object
                file.write(f"    {struct_name}.Start(builder)\n")
                for prop in struct.properties:
                    if prop in string_fields:
                        file.write(f"    {struct_name}.Add{prop.name}(builder, {prop.name}_off)\n")
                    elif prop in vector_fields:
                        file.write(f"    {struct_name}.Add{prop.name}(builder, {prop.name}_vec)\n")
                    else:
                        file.write(f"    {struct_name}.Add{prop.name}(builder, {prop.name}_val)\n")
                file.write(f"    return {struct_name}.End(builder)\n\n")

    def _get_conversion_code(self, prop, value_var):
        data_type = prop.data_type
        if data_type == "bool":
            return value_var, data_type
        if data_type in self.enums_by_name:
            return f"convert_int(getattr({data_type}, {value_var}), password)", "int"
        elif data_type == "float":
            return f"encrypt_float({value_var}, password)", data_type
        elif data_type == "double":
            return f"encrypt_double({value_var}, password)", data_type
        else:
            conversion_map = {
                "short": "convert_short",
                "ushort": "convert_ushort",
                "int": "convert_int",
                "uint": "convert_uint",
                "long": "convert_long",
                "ulong": "convert_ulong"
            }
            func = conversion_map.get(data_type, "convert_int")
            return f"{func}({value_var}, password)", data_type
