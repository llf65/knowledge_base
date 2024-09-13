import pdfplumber
from docx import Document
import docx2txt
import pandas as pd


class TextExtractor:  # 文本提取器
    @staticmethod
    def extract_text_from_pdf(file_obj):
        file_obj.seek(0)
        with pdfplumber.open(file_obj) as pdf:
            full_text = ''.join(
                [(page.extract_text() or '') + '\n' for page in pdf.pages if (page.extract_text() or '').strip() != ''])

        file_obj.seek(0)
        return full_text

    @staticmethod
    def extract_text_from_docx(file_obj):
        file_obj.seek(0)
        doc = Document(file_obj)
        full_text = '\n'.join([para.text + '\n' for para in doc.paragraphs if para.text.strip() != ''])
        file_obj.seek(0)
        return full_text

    @staticmethod
    def extract_text_from_doc(file_obj):
        file_obj.seek(0)
        with open("temp.doc", "wb") as f:
            f.write(file_obj.read())
        full_text = docx2txt.process("temp.doc")
        return full_text

    @staticmethod
    def extract_text_from_excel(file_obj, file_ext):
        file_obj.seek(0)
        engine = 'openpyxl' if file_ext == 'xlsx' else 'xlrd'
        xls = pd.ExcelFile(file_obj, engine=engine)
        full_text = '\n'.join(
            [str(cell).strip() + '\n'  # 确保转换为字符串并调用.strip()
             for sheet_name in xls.sheet_names
             for df in [pd.read_excel(xls, sheet_name)]
             for row in df.itertuples(index=False)
             for cell in row if str(cell).strip() != ''])  # 这里也确保是字符串
        file_obj.seek(0)
        return full_text.strip()

    @staticmethod
    def extract_text(file_obj, file_ext):
        if file_ext == 'pdf':
            return TextExtractor.extract_text_from_pdf(file_obj)
        elif file_ext == 'docx':
            return TextExtractor.extract_text_from_docx(file_obj)
        elif file_ext == 'doc':
            return TextExtractor.extract_text_from_doc(file_obj)
        elif file_ext in ['xlsx', 'xls']:
            return TextExtractor.extract_text_from_excel(file_obj, file_ext)
        else:
            raise ValueError("不支持的文件类型")

