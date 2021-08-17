from datetime import date
import pandas as pd
from urllib.parse import urlparse, parse_qs
import s3fs

class Quantifier:
    OUTPUT_PATH = 's3://merchant-hit-data/output'
    PURCHASE_EVENT = '1'

    def __init__(self, filename, output=None):
        self.filename = filename
        self.output_path = output if output is not None else self.OUTPUT_PATH
        
    def ExtractDomain(self, url):
        """Exctracts domain from URL
        Args:
            url(string)
        Returns:
            string
        """
        try:
            pr = urlparse(url)
            domain = pr.netloc
            domain= '.'.join(domain.split('.')[1:])
            return domain
        except:
            return
    
    def IsPurchased(self,event_list, product_list):
        """ Function to identify the purchased products
        Args:
            event_list (string)
            product_list (string)
        Returns:
            string or None
        """
        if isinstance(event_list, str):
            events = event_list.split(',')
            if self.PURCHASE_EVENT in events:
                return product_list
        return None

    def CalculateRevenue(self, product_list):
        """Function to calculate revenue from product list
        Args:
            product_list (string)
        Returns:
            Float|None
        """
        revenue = 0
        try:
            if isinstance(product_list, str):
                products = product_list.split(',')
                for product in products:
                    try:
                        attributes = product.split(';')
                        if len(attributes)>3:
                            revenue += float(product.split(';')[3]) * int(product.split(';')[2])
                    except Exception as e:
                        print('Malformed product list data {},error : {}'.format(product_list, e))
        except Exception as e:
            print('error processing data in Calculate Revenue for {},{}'.format(product_list, e))
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
            raise Exception("Error reading the input file {}".format(self.filename))
    
    def IsExternal(self,page_url,referrer):
        """Function to check if the refferer domain is same as the website
        Args:
            page_url (string)
            referrer (string)
        Returns:
            string|None:
        """
        try:
            domain = self.ExtractDomain(page_url)
            referrer_domain = self.ExtractDomain(referrer)
            if domain != referrer_domain:
                return referrer
        except Exception as e:
            print( "Error in checking if the domain is external {}, {} .Error: {}".format(page_url,referrer,e))
            return

    def TransformData(self, df):
        """ Function to tranform the input data to output 
        Args:
            df (dataframe object)
        Returns:
            dataframe object:
        """
        try:
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
        except Exception as e:
            print("Error transforming data {}".format(e))
    
    def ExtractSearchKey(self, url):
        """ Function to get the search key from search engine url
        Args:
            url (string)
        Returns:
            string|None
        """
        qs = ''
        try:
            pr = urlparse(url)
            search_engines = {'bing.com':'q',
                            'google.com':'q',
                            'yahoo.com':'p'}
            domain= self.ExtractDomain(url)
            if domain in search_engines:
                search_param = search_engines[domain]
                qs = parse_qs(pr.query)[search_param]
        except Exception as e:
            print("Error in exctractsearchkey {}".format(e))
        finally:
            return ''.join(qs).lower()
        
    def WriteToCSV(self, output):
        try:
            dt = date.today().strftime("%Y-%m-%d")
            output.to_csv('{}/{}_SearchKeywordPerformance.tab'.format(self.output_path, dt), sep='\t', index =False)
        except Exception as e:
            raise Exception("Failed writing error file {}".format(e))

