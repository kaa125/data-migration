import pandas as pd
import json

def get_retailerusers_df(db):
    # Get all records with only _id, city_id, and survey fields to save on memory
    docs_list = list(db.retailerusers.find({}, {"_id": 1, "city_id": 1, "survey":1}))
    df = pd.DataFrame(docs_list)  # Original DataFrame
    df.rename(columns={'_id': 'user_id'}, inplace=True)

    # Expanding surveys array (from MongoDB) to get each survey id assigned against a user id
    df_survey = df[['user_id', 'survey']]
    df_survey = df_survey.explode('survey').reset_index(drop=True)
    df_survey.set_index('user_id', inplace=True)
    df_survey = pd.json_normalize(df_survey['survey']).set_index(df_survey.index).reset_index()  # Converting dictionaries to columns with values
    df_survey.rename(columns={'_id': 'survey_id', 'name': 'survey_name', 'status': 'attempt_status'}, inplace=True)
    df_survey.drop(['survey_name', 'attempt_status'], axis=1, inplace=True)

    # Joining surveys with the original DataFrame
    # First dropping the survey column from the original DataFrame
    df.drop("survey", axis=1, inplace=True)
    df_final = df.merge(df_survey, on="user_id", how="inner")
    df_final.drop_duplicates(inplace=True)
    return df_final

def get_surveys_df(db):
    # Get all records
    docs_list = list(db.surveys.find())
    # Records as a DataFrame
    df_or = pd.DataFrame(docs_list)
    to_drop = ['push_notification_header', 'push_notification_text', 'owner', '__v']
    df_or.drop(to_drop, axis=1, inplace=True)

    # Questions DataFrame
    df_questions = df_or[['_id', 'questions']].copy()
    df_questions.rename(columns={'_id': 'parent_id'}, inplace=True)
    df_questions = df_questions.explode('questions').reset_index(drop=True)
    df_questions.set_index('parent_id', inplace=True)
    df_questions = pd.json_normalize(df_questions['questions']).set_index(df_questions.index).reset_index()
    df_questions.rename(columns={'_id': 'question_id'}, inplace=True)

    # For options array
    df_options = df_questions[['parent_id', 'question_id', 'options']].copy()
    df_options = df_options.explode('options').reset_index(drop=True)
    df_options.set_index(['parent_id', 'question_id'], inplace=True)
    df_options = pd.json_normalize(df_options['options']).set_index(df_options.index).reset_index()
    df_options.rename(columns={'_id': 'option_id'}, inplace=True)

    # Merging questions and options
    df_interim = df_questions.merge(df_options, on=["parent_id", "question_id"], how="inner")
    df_interim.drop(['options', 'selected_option', 'linked_follow_up_question_id', 'custom_attribute'], axis=1, inplace=True)
    
    # Merging required tables
    df_or.drop(['questions'], axis=1, inplace=True)
    df_final = df_or.merge(df_interim, left_on="_id", right_on="parent_id", how="inner")
    df_final.drop(['parent_id'], axis=1, inplace=True)
    df_final.rename(columns={'_id': 'survey_id', 'name': 'survey_name', 'city_id': 'cities_survey_offered'}, inplace=True)
    df_final['cities_survey_offered'] = df_final['cities_survey_offered'].astype('str')
    df_final.drop('file_id', axis=1, inplace=True)
    df_final.drop_duplicates(inplace=True)
    df_final['survey_created'] = pd.to_datetime(df_final['survey_created'], unit='s')
    df_final['survey_updated'] = pd.to_datetime(df_final['survey_updated'], unit='s')
    df_final['survey_started'] = pd.to_datetime(df_final['survey_started'], unit='s')
    df_final['survey_ended'] = pd.to_datetime(df_final['survey_ended'], unit='s')
    return df_final

def transform_dictionary(dic):
    '''
    Function to transform nested dictionary into a single-layer dictionary {'key':'val'} to feed to json_normalize()
    '''
    temp = {}
    if len(dic['answer']) != 0:
        item = dic['answer'][0]
        temp['question_id'] = item['_id']
        temp['options'] = item['options']
    else:
        temp = {}
    return temp

def transform_option_text(in_obj):
    return_dict = {}
    if isinstance(in_obj, dict):
        return_dict['_id'] = in_obj['_id']
        return_dict['option'] = in_obj['option']
    else:
        return_dict['_id'] = None
        return_dict['option'] = in_obj
    return return_dict

def get_results_df(db):
    docs_list = list(db.results.find())
    df_or = pd.DataFrame(docs_list)
    df_or.rename(columns={'_id': 'result_id'}, inplace=True)

    # Unnesting answers
    df_ans = df_or[['result_id', 'answers']]
    df_ans = df_ans.explode('answers').reset_index(drop=True)
    df_ans.set_index('result_id', inplace=True)
    df_ans = df_ans[df_ans['answers'].map(lambda d: len(d['answer'])) > 0]
    df_ans['answers'] = df_ans['answers'].apply(transform_dictionary)
    df_ans = pd.json_normalize(df_ans['answers']).set_index(df_ans.index).reset_index()

    # Working to unnest options lists if they exist
    df_options = df_ans[['result_id', 'question_id', 'options']]
    df_options.dropna(subset=['options'], axis=0, inplace=True)
    df_options.set_index(['result_id', 'question_id'], inplace=True)
    df_options = df_options.explode('options')
    df_options['options'] = df_options['options'].apply(transform_option_text)
    df_options = pd.json_normalize(df_options['options']).set_index(df_options.index).reset_index()
    df_options.rename(columns={'_id': 'options._id', 'option': 'options.option'}, inplace=True)

    # Merging answers and options DataFrames
    df_interim = df_ans.merge(df_options, on=['result_id', 'question_id'], how='left')
    df_interim['options._id_x'].fillna(df_interim['options._id_y'], inplace=True)
    df_interim['options.option_x'].fillna(df_interim['options.option_y'], inplace=True)
    df_interim.drop(['options', 'options._id_y', 'options.option_y'], axis=1, inplace=True)
    df_interim.rename(columns={'options._id_x': 'option_id', 'options.option_x': 'selected_option'}, inplace=True)

    # Final DataFrame
    df_final = df_or.merge(df_interim, on="result_id", how='inner')
    df_final.drop(['answers', '__v'], axis=1, inplace=True)
    df_final['user_id'] = df_final['user_id'].astype('int')
    df_final.drop_duplicates(inplace=True)
    df_final['created_at'] = pd.to_datetime(df_final['created_at'], unit='s')
    df_final['updated_at'] = pd.to_datetime(df_final['updated_at'], unit='s')
    return df_final
