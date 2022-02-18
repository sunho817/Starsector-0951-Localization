import dataclasses
import json
import logging
import pprint
import re
from _csv import writer
from csv import DictReader, reader
from dataclasses import dataclass
from pathlib import Path

from typing import Set, Dict, Tuple, List, Union

# 设置日志输出
logging.root.setLevel(logging.NOTSET)
logger = logging.getLogger('ParaTranz.py')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter("[%(name)s][%(levelname)s] %(message)s")

ch.setFormatter(formatter)
logger.addHandler(ch)

# 设置游戏原文，译文和Paratranz数据文件路径
PROJECT_DIRECTORY = Path(__file__).parent.parent
ORIGINAL_PATH = PROJECT_DIRECTORY / 'original'
TRANSLATION_PATH = PROJECT_DIRECTORY / 'localization'
PARA_TRANZ_PATH = PROJECT_DIRECTORY / 'para_tranz' / 'output'
CONFIG_PATH = PROJECT_DIRECTORY / 'para_tranz' / 'para_tranz_map.json'


# 尝试计算相对于根目录的位置
def relative_path(path: Path) -> Path:
    try:
        return path.relative_to(PROJECT_DIRECTORY)
    except Exception as _:
        return path


@dataclass
class String:
    key: str
    original: str
    translation: str
    stage: int = 0  # status，0 untranslated，1 first pass，2 uncertain，3 first check，5 second check，9 final，-1 hide
    context: str = ''  # entry note

    def __post_init__(self):
        # when importing json from ParaTranz, replace \\n with \n
        # do not use \\n in the resulting json export. ^n instead
        self.original = self.original.replace('\\n', '\n')
        self.translation = self.translation.replace('\\n', '\n')

    def as_dict(self) -> Dict:
        return dataclasses.asdict(self)


# original and localized file
class DataFile:
    def __init__(self, path: Path, original_path: Path = None, translation_path: Path = None):
        self.path = Path(path)  # relative filepath for original and localization folders
        self.original_path = ORIGINAL_PATH / Path(original_path if original_path else path)
        self.translation_path = TRANSLATION_PATH / Path(
            translation_path if translation_path else path)
        self.para_tranz_path = PARA_TRANZ_PATH / self.path.with_suffix('.json')

    def get_strings(self) -> Set[String]:
        pass

    def update_strings(self, strings: Set[String]):
        pass


# translate to CSV
class CsvFile(DataFile):
    def __init__(self, path: Path, id_column_name: str, text_column_names: Set[str],
                 original_path: Path = None, translation_path: Path = None):
        super().__init__(path, original_path, translation_path)
        # name of ID column in CSV
        self.id_column_name = id_column_name  # type:Union[str, List[str]] # May be multiple
        self.text_column_names = text_column_names  # type:Set[str]  # column names with text that needs translating

        self.column_names = []  # type:List[str]

        # map data from original data and id
        self.original_data = []  # type:List[Dict]
        self.original_id_data = {}  # type:Dict[str, Dict]
        # map data from translated data and id
        self.translation_data = []  # type:List[Dict]
        self.translation_id_data = {}  # type:Dict[str, Dict]

        self.load_original_and_translation_data()

        self.validate()

    # read from source and target CSV
    def load_original_and_translation_data(self) -> None:
        self.column_names, self.original_data, self.original_id_data = self.load_csv(
            self.original_path,
            self.id_column_name)
        logger.info(
            f'From {relative_path(self.original_path)} loaded {len(self.original_data)}. Original data. Number of lines not empty or commented: {len(self.original_id_data)}')
        if self.translation_path.exists():
            _, self.translation_data, self.translation_id_data = self.load_csv(
                self.translation_path,
                self.id_column_name)
            logger.info(
                f'From {relative_path(self.translation_path)} loaded {len(self.translation_data)} Translated data. Number of lines not empty or commented: {len(self.translation_id_data)}')

    # validate data after reading
    def validate(self):
        # verify if the specified id and text column exists in game file
        if (type(self.id_column_name) == str and self.id_column_name not in self.column_names) and (
                not set(self.id_column_name).issubset(set(self.column_names))):
            raise ValueError(
                f'{self.path} does not contain ID column "{self.id_column_name}"，Check config file. Usable columns： {self.column_names}')
        if not set(self.text_column_names).issubset(set(self.column_names)):
            raise ValueError(
                f'{self.path} does not contain text column {self.text_column_names}，Check config file. Usable columns： {self.column_names}')
        # check size
        if len(self.original_data) != len(self.translation_data):
            logger.warning(
                f'Original file {relative_path(self.path)} does not match size with translated data: {len(self.original_data)} items instead of {len(self.translation_data)} items translated.')
        if len(self.original_id_data) != len(self.translation_id_data):
            logger.warning(
                f'Original {relative_path(self.path)} contain number of not empty or commented items differ from translated data: {len(self.original_id_data)} valid items instead of {len(self.translation_id_data)} translated.')

    # convert data to Paratranz data entity
    def get_strings(self) -> List[String]:
        strings = []
        for row_id, row in self.original_id_data.items():

            # only export items that are neither empty nor commented
            first_column = row[list(row.keys())[0]]
            if not first_column or first_column[0] == '#':
                continue

            context = self.generate_row_context(row)
            for col in self.text_column_names:
                key = f'{self.path.name}#{row_id}${col}'  # item id format: <filename>-<column id>-<column name>
                original = row[col]
                translation = ''
                stage = 0
                if row_id in self.translation_id_data:
                    translation = self.translation_id_data[row_id][col]
                    stage = 1
                # Special rules for rules.csv: if "script" column does not contain ' " ' it counts as translated
                if (self.path.name == 'rules.csv') and (col == 'script') and (
                        '"' not in original):
                    stage = 1
                # set untranslated if it is not translated
                elif not contains_korean(translation):
                    translation = ''
                    stage = 0

                strings.append(String(key, original, translation, stage, context))
        return strings

    # generate row context depending on the row id
    def generate_row_context(self, row: dict) -> str:
        row_num = self.original_data.index(row)

        return f"{self.path.name} row {str(row_num + 1).zfill(4)}\n[original data]\n{pprint.pformat(row, sort_dicts=False)}"

    # convert data to ParaTranz item data entry and save to json file
    def save_strings_json(self, ensure_ascii=False, indent=4) -> None:
        strings = [s for s in self.get_strings() if s.original]  # only export items that are not empty

        # if Paratranz json already exists, synchronize the status of already translated items
        if self.para_tranz_path.exists():
            logger.info(f"Paratranz data file {relative_path(self.para_tranz_path)} already exists, reading already translated items")

            special_stages = (1, 2, 3, 5, 9, -1)
            para_strings = self.load_json_strings(self.para_tranz_path)
            para_key_strings = {s.key: s for s in para_strings if
                                s.stage in special_stages}  # type:Dict[str, String]
            for s in strings:
                if s.key in para_key_strings:
                    para_s = para_key_strings[s.key]
                    if s.stage != para_s.stage:
                        logger.debug(f"{s.key} stage updated from {s.stage} to {para_s.stage}")
                        s.stage = para_s.stage

        # export Paratranz items
        self.para_tranz_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.para_tranz_path, 'w', encoding='utf-8') as f:
            data = []
            for string in strings:
                data.append(string.as_dict())
            json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent)

        logger.info(
            f'From {relative_path(self.path)} exported {len(strings)} items to {relative_path(self.para_tranz_path)}')

    # Merge incoming Paratranz data to preexisting data
    def update_strings(self, strings: List[String]) -> None:
        for s in strings:
            _, id, column = re.split('[#$]', s.key)
            if id in self.translation_id_data:
                # If it is translated and not empty, overwrite with newer version
                if s.stage > 0 and s.translation:
                    self.translation_id_data[id][column] = s.translation
                elif contains_korean(self.translation_id_data[id][column]):
                    logger.warning(f'From {self.path}, row {self.id_column_name}="{id}" is already translated.'
                                   f'Item absent in newer version. Item remains unchanged.')
            else:
                logger.warning(f'Cannot find {self.id_column_name}="{id}" in {self.path}. Item is not updated.')

    # read Paratranz data from json file and merge to existing data
    def update_strings_from_json(self) -> None:
        if self.para_tranz_path.exists():
            strings = self.load_json_strings(self.para_tranz_path)
            self.update_strings(strings)
            logger.info(
                f'From {relative_path(self.para_tranz_path)} exported {len(strings)} items to {relative_path(self.translation_path)}')
        else:
            logger.warning(f'{self.path} does not have corresponding ParaTranz data ({self.para_tranz_path}). File is not updated.')

    # rewrite data to translated CSVs
    def save_translation_data(self) -> None:
        with open(self.translation_path, 'r', newline='', encoding='utf-8') as f:
            csv = reader(f)
            real_column_names = csv.__next__()

        # CSV may contain empty columns which will be deleted when reading with DictReader; will be re-added later for consistency with source file
        real_column_index = {col: real_column_names.index(col) for col in self.column_names if col}

        rows = [real_column_names]

        for dict_row in self.translation_data:
            row = ['' for _ in range(len(real_column_names))]
            for col, value in dict_row.items():
                if col:
                    # to avoid the entire file getting a newline(LF), replace \n with \r\n
                    # replace ^n, only used when reading csv, back to \\n
                    value = value.replace('^n', '\\n').replace('\n', '\r\n')
                    row[real_column_index[col]] = value
            rows.append(row)
        with open(self.translation_path, 'w', newline='', encoding='utf-8') as f:
            writer(f).writerows(rows)

    @staticmethod
    def load_json_strings(path: Path) -> List[String]:
        strings = []
        with open(path, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)  # type:List[Dict]
        for d in data:
            strings.append(
                String(d['key'], d['original'], d.get('translation', ''), d['stage']))
        return strings

    @staticmethod
    def load_csv(path: Path, id_column_name: Union[str, List[str]]) -> Tuple[
        List[str], List[Dict], Dict[str, Dict]]:
        """
        Reads data from CSV. Returns column name, data, and list of id column mapped to data
        :param path: csv file path
        :param id_column_name: name of ID column. pass ID if one, list if multiple
        :return: (column name list, data list, id column contents to data map dict)
        """
        data = []
        id_data = {}
        with open(path, 'r', errors="surrogateescape", encoding='utf-8') as csv_file:
            # replace unreadable characters and replace \n with ^n to differentiate from CSV newline
            csv_lines = [replace_weird_chars(l).replace('\\n', '^n') for l in csv_file]
            rows = list(DictReader(csv_lines))
            columns = list(rows[0].keys())
            for i, row in enumerate(rows):
                if type(id_column_name) == str:
                    row_id = row[id_column_name]  # type:str
                else:  # multiple id columns
                    row_id = str(tuple([row[id] for id in id_column_name]))  # type:str

                # check inline data length against file size
                for col in row:
                    if row[col] is None:
                        row[col] = ''
                        logger.warning(
                            f'File {path}, row {i}, {id_column_name}="{row_id}" has insufficient value count. May have a missing comma.')

                first_column = row[columns[0]]
                # save only non-null and uncommented rows from id-row mapping
                if first_column and not first_column[0] == '#':
                    if row_id in id_data:
                        raise ValueError(f'File {path}, row {i}, {id_column_name}="{row_id}" is not unique in the file.')
                    id_data[row_id] = row
                data.append(row)
        return columns, data, id_data


# https://segmentfault.com/a/1190000017940752
# see if it contains Chinese characters
def contains_chinese(s: str) -> bool:
    for _char in s:
        if '\u4e00' <= _char <= '\u9fa5':
            return True
    return False

def contains_korean(s: str) -> bool:
    for _char in s:
        if '\u1100' <= _char <= '\u11ff':
            return True
    return False


def load_csv_files() -> List[CsvFile]:
    """
    read original and translated CSV files according to rules specified in para_tranz_map.json
    :return: CsvFile list
    para_tranz_map.json format is as follows：
    [
        {
            "path": "csv file path，use '/' for delimiter and include '.csv'",
            "id_column_name": "CSV id column names",
            "text_column_names": [
              "column name 1",
              "column name 2"
            ]
        },
        {
            "path": "csv file path，use '/' for delimiter and include '.csv'",
            "id_column_name": ["作为id的列名1", "作为id的列名2"],
            "text_column_names": [
              "需要翻译列的列名1",
              "需要翻译列的列名2"
            ]
        }
    ]
    """
    logger.info('Reading original and translated data...')
    with open(CONFIG_PATH, encoding='utf-8') as f:
        d = json.load(f)
    files = [CsvFile(**mapping) for mapping in d]
    logger.info('Finished reading original and translated data.')
    return files


def csv_to_paratranz():
    for file in load_csv_files():
        file.save_strings_json()
    logger.info('Finished exporting ParaTranz entry')


def paratranz_to_csv():
    for file in load_csv_files():
        file.update_strings_from_json()
        file.save_translation_data()
    logger.info('Finished importing from ParaTranz data')


# From processWithWiredChars.py
# Original file may contain Windows-1252 encoded strings that needs to be replaced
def replace_weird_chars(s: str) -> str:
    return s.replace('\udc94', '""') \
        .replace('\udc93', '""') \
        .replace('\udc92', "'") \
        .replace('\udc91', "'") \
        .replace('\udc96', "-") \
        .replace('\udc85', '...')


if __name__ == '__main__':
    print('Welcome to Paratranz import/export tool.')
    print('Choose the operation to perform：')
    print('1 - Export Paratranz data from original and translated files')
    print('2 - Import Paratranz data to translated files')

    while True:
        option = int(input('Input option number：'))
        if option == 1:
            csv_to_paratranz()
            break
        elif option == 2:
            paratranz_to_csv()
            break
        else:
            print('Invalid option!')

    input('Press any key to exit')
