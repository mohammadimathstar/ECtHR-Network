# Code to load and preprocess the CSV file.
import pandas as pd
from typing import Literal
from collections import Counter
import re


class CaseDataPreprocessor:
    """
    A class to preprocess and clean the DataFrame of case laws presented in ECtHR.
    """
    def __init__(self, doctype: Literal["DECISIONS", 'JUDGMENTS']):
        """
        Initializes the preprocessor with the provided type of case law document.

        Parameters:
        - doctype (Literal["DECISIONS", 'JUDGMENTS']): Type of case law document ('DECISIONS' or 'JUDGMENTS').
        """
        file_path = f"./data/download/{doctype}_full.csv"
        self.df = pd.read_csv(file_path)

        # Dictionary to rename DataFrame columns
        self.rename = {"appno": "AppNo", 'docname': 'Title', 'doctype': 'DocType',
                       'importance': 'ImportanceLevel', 'extractedappno': 'ReferTo',
                       'originatingbody': 'OriginatingBody', 'doctypebranch': 'Chamber',
                       'respondent': 'Respondent', 'conclusion': 'Conclusion',
                       'article': "Article", 'kpdate': "JudgmentDate", 'language': 'Language', 'text': 'Text'}

        # Drop unnecessary column
        self.df.drop('appnoyear.1', axis=1, inplace=True)
        print(f"There are {len(self.df)} cases. {self.df.text.isna().sum()} cases do not have text.")

    def drop_unnecessary_columns(self):
        """
        Drops unnecessary columns from the DataFrame.
        """
        print(f"We keep the following entries: \n\t{self.rename.keys()}")
        print(f"\nWe remove the following entries: \n\t{set(self.df.columns) - set(self.rename.keys())}")
        return self.df[self.rename.keys()]

    def rename_columns(self):
        """
        Renames columns of the DataFrame according to the specified mapping.
        """
        self.df.rename(columns=self.rename, inplace=True)

    def update_categorical_variables(self):
        """
        Updates values of categorical variables in the DataFrame.
        """
        # Mapping dictionaries for categorical variables
        dic_imp = {1: 'Key Case', 2: 1, 3: 2, 4: 3}
        dic_org_body = {4: 'Court (first section)', 5: 'Court (second section)', 6: 'Court (third section)',
                        7: 'Court (fourth section)', 8: 'Court (grand chamber)', 9: 'Court (chamber)',
                        15: 'Court (plenary)', 23: 'Court (fifth section)', 25: 'Court (first section committee)',
                        26: "Court (second section committee)", 27: "Court (third section committee)",
                        28: "Court (fourth section committee)", 29: "Court (fifth section committee)"}
        dic_doc_type = {'HEJUD': "JUDGMENT", "HEDEC": "DECISION", "HEJP9": "JP9", "HECOM": "COMMUNICATED_CASE"}

        # Replace values of categorical variables
        self.df.replace({"ImportanceLevel": dic_imp, "OriginatingBody": dic_org_body, "DocType": dic_doc_type},
                        inplace=True)

    def remove_self_references(self):
        """
        Removes self-references in the 'ReferTo' column of the DataFrame.
        """
        for i, (appno, refs) in self.df[['AppNo', 'ReferTo']].iterrows():
            if isinstance(appno, str) and isinstance(refs, str) and refs != "":
                for s in appno.split(';'):
                    self.df.loc[i, 'ReferTo'] = self.df.loc[i, 'ReferTo'].replace(s, "")
                    l = [p for p in self.df.loc[i, 'ReferTo'].split(';') if p != ""]
                    self.df.loc[i, 'ReferTo'] = ";".join(l)

    def preprocess_data(self):
        """
        Performs data preprocessing steps.
        """
        # Drop unnecessary columns
        self.drop_unnecessary_columns()

        # Rename columns
        self.rename_columns()

        # Update categorical variables
        self.update_categorical_variables()

        # Remove self-references
        self.remove_self_references()
        return self.df

# pre = preprocess('DECISIONS')
# pre.do_preprocess()