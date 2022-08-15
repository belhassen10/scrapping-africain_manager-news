# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 15:11:07 2022

@author: Amdoun
"""

import scrapy

import pandas as pd
import os 
import spacy
from spacy.lang.fr import French
from spacy.lang.fr.stop_words import STOP_WORDS
from collections import Counter
import re



items=[]
 
class ScrapeRssSpider(scrapy.Spider):
    name = 'africain-rss'
    allowed_domains = ['https://africanmanager.com/feed/']
    start_urls = ['https://africanmanager.com/feed/']
 
    def start_requests(self):
        urls = [
            'https://africanmanager.com/feed/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
 
    def parse(self, response):
        response.selector.remove_namespaces() ##Remove XML namespaces
        
        for post in response.xpath('//channel/item'):
            item ={
                'title' : post.xpath('title//text()').extract_first(),
                'link': post.xpath('link//text()').extract_first(),
                'author' : post.xpath('creator//text()').extract_first(),
                'pubDate' : post.xpath('pubDate//text()').extract_first(),
                'description' : post.xpath('description//text()').extract_first(),
                'text': post.xpath('encoded//text()').extract_first(),
             }
            items.append(item)
            
   
        
            
    def closed(self ,reason):
        '''Method to be called after the spider finishes'''
        # Loads the spaCy small French language model
        nlp = spacy.load("fr_core_news_sm")
        # Removes stopwords from spaCy default stopword list
        nlp.Defaults.stop_words -= {"my_stopword_1", "my_stopword_2"}
        # Adds custom stopword into spaCy default stopword list
        nlp.Defaults.stop_words |= {"my_stopword_1", "my_stopword_2"}
        print(nlp.Defaults)
        def remove_punctuation_special_chars(sentence):
            nlp = spacy.load("fr_core_news_sm")
            sentence = nlp(sentence)
            processed_sentence = ' '.join([token.text for token in sentence if token.is_punct != True and 
                                           token.is_quote != True and 
                                           token.is_bracket != True and 
                                           token.is_currency != True and 
                                           token.is_digit != True])
            return processed_sentence
        
        def remove_stop_words(sentence):
            nlp = spacy.load("fr_core_news_sm")
            sentence = nlp(sentence)
            processed_sentence=' '.join([token.text for token in sentence if token.is_stop != True])
            
            return processed_sentence
            
        
        def lemmatize_text(sentence):
            nlp = spacy.load("fr_core_news_sm")
            sentence = nlp(sentence)
            processed_sentence = ' '.join([word.lemma_ for word in sentence])
            return processed_sentence
        
        def remove_bad_chars(text):
          bad_chars=['%', '#', '"', '*']
          for i in bad_chars:
              text=text.replace(i,'')
          return text
      
        def split_sentences(document):
            nlp = spacy.load("fr_core_news_sm")
            doc=nlp(document)
            sentences = [sent.text.strip() for sent in doc.sents]
            return sentences
        
        for i in range((len(items))):
            for p in ['title','author','pubDate','description']:              #,'text']:
                items[i][p]=remove_punctuation_special_chars(items[i][p])
                items[i][p]=split_sentences(items[i][p])
                
                for k in range(len(items[i][p])):
                    
                    items[i][p][k]=remove_bad_chars(items[i][p][k])
                    items[i][p][k]=remove_stop_words(items[i][p][k])
                    items[i][p][k]=lemmatize_text(items[i][p][k])
                
                
                
            
        print('pre-processing finish')
        
        
        

        # create datafrane from the global list 'items'
        df = pd.DataFrame(items, columns=['title', 'link','author','pubDate','description','text'])
        
        
        filename= r'C:\Users\Amdoun\Desktop\Stage\africain_manager\africain_manager\africanmanager.csv'
        if os.path.exists(filename):
            print('fichier existe , data ajouté avec succée')
            df.to_csv('africanmanager.csv', mode='a', index=False)
        else :
            df.to_csv('africanmanager.csv', index=False ) 
            
    

               
        
