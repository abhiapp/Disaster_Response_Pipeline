# load Required Libraries for ETL Pipeline
import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    '''
    INPUT:
        messages_filepath - Location of message data
        categories_filepath - Location of categories data
    
    OUTPUT:
        df - Return a DataFrame that combines the data from messages and catagories datasets.
    '''
    # load messages dataset
    messages = pd.read_csv(messages_filepath)
    
    # load categories dataset
    categories = pd.read_csv(categories_filepath)
    
    # merge both datasets
    df = pd.merge(messages, categories, how='left', on='id')
    
    return df


def clean_data(df):
    '''
    INPUT:
        df - A Data Frame combining Message and categories dataset
        
    OUTPUT:
        df - Return a Cleand DataFrame
    '''
    # Organize Categories
    # Split `categories` into separate category columns.
    categories = df['categories'].str.split(';', expand=True)
    # select the first row of the categories dataframe
    row = df.categories[0]
    # up to the second to last character of each string with slicing
    category_colnames = [column.split('-')[0] for column in row.split(';')]
    # rename the columns of `categories`
    categories.columns = category_colnames
    
    # Clean Categories
    # Convert category values to just numbers 0 or 1.
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1]
    
        # convert column from string to numeric
        categories[column] = categories[column].astype(int)    
    # drop the original categories column from `df`
    df.drop(columns=['categories'], inplace=True)
    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)
    
    #Remove Duplicates
    df = df.drop_duplicates()
    
    return df


def save_data(df, database_filename):
    '''
    INPUT:
        df -  A cleaned Dataframe 
        database_filename -  File Name for SQL Lite Database
    OUTPUT: None
         A SQL Lite Database File (DisasterResponse.db) will be created
    ''' 
    #Create Databse Engine
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('DisasterResponse', engine, index=False, if_exists='replace')


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()