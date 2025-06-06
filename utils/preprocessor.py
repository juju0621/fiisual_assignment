import re
import pandas as pd
import numpy as np

class StockDataPreprocessor:
    def __init__(self, ticker, update_date):
        self.ticker = ticker
        self.update_date = update_date

    @staticmethod
    def convert_arrow_to_float(x):
        if isinstance(x, str):
            if '↗' in x:
                return float(x.replace('↗', ''))
            elif '↘' in x:
                return -float(x.replace('↘', ''))
            else:
                return float(x)
        return x

    @staticmethod
    def convert_chinese_number(s):
        if isinstance(s, str):
            s = s.replace(',', '')
            if '兆' in s:
                return float(s.replace('兆', '')) * 1e12
            elif '億' in s:
                return float(s.replace('億', '')) * 1e8
            elif '萬元' in s:
                return float(s.replace('萬元', '')) * 1e4
            else:
                try:
                    return float(s)
                except:
                    return None
        return s

    @staticmethod
    def clean_date(s):
        if isinstance(s, str):
            s = re.sub(r'\xa0.*', '', s)
            s = s.strip()
        return pd.to_datetime(s, errors="coerce")

    @staticmethod
    def clean_salary(s):
        if isinstance(s, str):
            s = s.replace('\u2002', '')
            match = re.search(r'([\d\.]+)', s)
            if match:
                return float(match.group(1)) * 1e4
        return None

    def process_statistics_data(self, df1, df2):
        df1.columns = df1.iloc[0]
        df1 = df1[1:]

        df2 = df2.copy()
        df2.loc[df2["統計  區間"] == "月", "統計  區間"] = "1個月"
        df2.loc[df2["統計  區間"] == "季", "統計  區間"] = "3個月"
        df2.loc[df2["統計  區間"] == "年", "統計  區間"] = "1年"
        df2 = df2.set_index("統計  區間")
        df2.loc["5年"] = [np.nan] * df2.shape[1]

        df = pd.concat([df1, df2], axis=1)
        df.columns = [
            "Beta", "標準差", "年化標準差", "累計漲跌價", "累計漲跌幅（%）",
            "區間振幅（%）", "成交量週轉率（%）", "均線落點", "均線乖離率（%）"
        ]
        df.insert(0, "股票代號", self.ticker)
        df.insert(0, "更新日期", self.update_date)

        cols_to_float = ["Beta", "標準差", "年化標準差", "累計漲跌價"]
        cols_percentage_to_float = [
            "累計漲跌幅（%）", "區間振幅（%）", "成交量週轉率（%）", "均線乖離率（%）"
        ]
        df[cols_to_float] = df[cols_to_float].astype(float)
        df[cols_percentage_to_float] = df[cols_percentage_to_float].apply(
            lambda x: x.str.replace('%', '').astype(float)
        )

        df["均線落點"] = df["均線落點"].apply(self.convert_arrow_to_float)
        df["區間"] = df.index
        df.insert(0, "區間", df.pop("區間"))
        df = df.reset_index(drop=True)
        return df

    def process_info_data(self, info):
        info["股票代號"] = self.ticker
        info_df = pd.DataFrame([info])

        info_df['資本額'] = info_df['資本額'].apply(self.convert_chinese_number)
        info_df['市值'] = info_df['市值'].apply(self.convert_chinese_number)
        info_df['面值'] = info_df['面值'].str.extract(r'(\d+)', expand=False).astype(float)
        info_df['成立日'] = info_df['成立日'].apply(self.clean_date)
        info_df['掛牌日'] = info_df['掛牌日'].apply(self.clean_date)
        info_df['員工平均年薪(全體員工)'] = info_df['員工平均年薪(全體員工)'].apply(self.clean_salary)

        columns_to_convert = ["公司債", "私募股", "特別股"]
        info_df[columns_to_convert] = info_df[columns_to_convert] == "有"

        return info_df
