from os import error
from datetime import date
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import s3fs

class Quantifier:
    def __init__(self, filename):
        self.path = Path(__file__).parent
        self.filename = filename
        
    def ExtractDomain(self, url):
        pr = urlparse(url)
        domain = pr.netloc
        domain= '.'.join(domain.split('.')[1:])
        return domain
    
    def IsPurchased(self,event_list, product_list):
        if isinstance(event_list, str):
            events = event_list.split(',')
            if '1' in events:
                return product_list
        return None
        
    
    def CalculateRevenue(self, product_list):
        revenue = 0
        try:
            if isinstance(product_list, str):
                products = product_list.split(',')
                for product in products:
                    try:
                        attributes = product.split(';')
                        if len(attributes)>3:
                            revenue += float(product.split(';')[3]) * int(product.split(';')[2])
                    except:
                        print('error')
        except:
            print('error processing data')
        finally:
            if revenue == 0:
                return
            return revenue
    
    def ReadFile(self):
        try:
            fields =['product_list','referrer','page_url','event_list','ip']
            df = pd.read_csv(self.filename, sep='\t', usecols=fields, dtype='str')
            return df
        except Exception as e:
            raise Exception("Error reading the input file, please check the file format, the file should be json")
    
    def IsExternal(self,page_url,referrer):
        domain = self.ExtractDomain(page_url)
        referrer_domain = self.ExtractDomain(referrer)
        if domain != referrer_domain:
            return referrer
        return


    def TransformData(self, df):
        df['referrer'] = df.apply(lambda x: self.IsExternal(x.page_url, x.referrer), axis=1)
        df['product_list'] = df.apply(lambda x: self.IsPurchased(x.event_list, x.product_list), axis=1)
        df = df.groupby('ip',as_index=False).agg(lambda x: list(x[x.notna()]))
        df = df[['referrer','product_list']]
        df = df.apply(lambda x: x.apply(pd.Series).stack())
        df['Search Engine Domain']= df['referrer'].apply(self.ExtractDomain)
        df['Search Keyword']= df['referrer'].apply(self.ExtractSearchKey)
        df['Revenue'] = df['product_list'].apply(self.CalculateRevenue) 
        df = df[['Search Engine Domain','Search Keyword','Revenue']] 
        df = df.dropna().groupby(['Search Engine Domain','Search Keyword'],as_index=False ).sum()
        return df.sort_values(by='Revenue',ascending=False)
    
    def ExtractSearchKey(self, url):
        pr = urlparse(url)
        qs = ''
        search_engines = {'bing.com':'q',
                          'google.com':'q',
                          'yahoo.com':'p'}
        domain= self.ExtractDomain(url)
        if domain in search_engines:
            search_param = search_engines[domain]
            qs = parse_qs(pr.query)[search_param]
        return ''.join(qs).lower()
        
    def WriteToCSV(self, output):
        try:
            dt = date.today().strftime("%Y-%m-%d")
            print('s3://zebtest/{}_SearchKeywordPerformance.tab'.format(dt))
            output.to_csv('s3://zebtest/{}_SearchKeywordPerformance.tab'.format(dt), sep='\t', index =False)
        except Exception as e:
            raise Exception("Failed writing error file {}".format(e))

