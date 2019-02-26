from io import BytesIO
import numpy as np
import pandas as pd
import psycopg2
from flask import Flask, render_template,request,send_file, make_response,json
from flask_bootstrap import Bootstrap
from pandas.io.json import json_normalize
from flask_cors import  CORS,cross_origin
app = Flask(__name__)
CORS(app, support_credentials=True)
Bootstrap(app)
con = psycopg2.connect("dbname=daabi6mhbsu5fm user=ewyivrzjhyxxyy password=dc37c6729bd76a50666bfc9ffad4fa11b6e3a1974834b3b5ab6933f23a25254d host=ec2-54-83-17-151.compute-1.amazonaws.com")
@app.route('/app1',methods=['GET', 'POST'])
@cross_origin(supports_credentials=True)
def index():
    if request.method=='GET':
        try:
            if isinstance(int(request.args.get('lid')),int)==True:
                num1 = request.args.get('trid')
                num1 = int(num1)
                cur=con.cursor()
                sql="select TraineeID,DocID,rating from tmstaskdata where TraineeID=%s LIMIT 1"
                cur.execute(sql,([num1]))
                if cur.fetchone()!=None:
                    sql="SELECT * FROM Tmsdocdata "
                    doc_df = pd.io.sql.read_sql(sql, con)
                    #print(doc_df)
                    sql="SELECT * FROM tmstaskdata "
                    rating_df = pd.io.sql.read_sql(sql, con)
                    sql="select TraineeID,DocID,rating from tmstaskdata where TraineeID=3"
                    #cursor=con.cursor()
                    userInput = pd.io.sql.read_sql(sql, con)
                    inputId = doc_df[doc_df['DocID'].isin(userInput['DocID'].tolist())]
                    print(inputId)
                    inputdoc = pd.merge(userInput, inputId)
                    print(inputdoc['DocID'])
                    #inputdoc = inputdoc.drop('year', 1)
                    #print(inputdoc)
                    userSubset = rating_df[rating_df['DocID'].isin(inputdoc['DocID'].tolist())]
                    #print(userSubset)
                    userSubsetGroup = userSubset.groupby(['TraineeID'])
                    ##print(userSubsetGroup)
                    #print(userSubsetGroup.get_group(10))
                    userSubsetGroup = sorted(userSubsetGroup,  key=lambda x: len(x[1]), reverse=True)
                    print(userSubsetGroup[0:10])
                    userSubsetGroup = userSubsetGroup[0:8]
                    print(userSubsetGroup)
                    pearsonCorrelationDict = {}

                    #For every user group in our subset
                    for name, group in userSubsetGroup:
                        #Let's start by sorting the input and current user group so the values aren't mixed up later on
                        group = group.sort_values(by='DocID')
                        inputdoc = inputdoc.sort_values(by='DocID')
                        #Get the N for the formula
                        nRatings = len(group)
                        #Get the review scores for the movies that they both have in common
                        temp_df = inputdoc[inputdoc['DocID'].isin(group['DocID'].tolist())]
                        #And then store them in a temporary buffer variable in a list format to facilitate future calculations
                        tempRatingList = temp_df['rating'].tolist()
                        #Let's also put the current user group reviews in a list format
                        tempGroupList = group['rating'].tolist()
                        #Now let's calculate the pearson correlation between two users, so called, x and y
                        Sxx = sum([i**2 for i in tempRatingList]) - pow(sum(tempRatingList),2)/float(nRatings)
                        Syy = sum([i**2 for i in tempGroupList]) - pow(sum(tempGroupList),2)/float(nRatings)
                        print(Syy)
                        Sxy = sum( i*j for i, j in zip(tempRatingList, tempGroupList)) - sum(tempRatingList)*sum(tempGroupList)/float(nRatings)
                        #If the denominator is different than zero, then divide, else, 0 correlation.
                        if Sxx != 0 and Syy != 0:
                            pearsonCorrelationDict[name] = Sxy/sqrt(Sxx*Syy)
                        else:
                            pearsonCorrelationDict[name] = 0
                    print(pearsonCorrelationDict.items())
                    pearsonDF = pd.DataFrame.from_dict(pearsonCorrelationDict, orient='index')
                    pearsonDF.columns = ['similarityIndex']
                    pearsonDF['TraineeID'] = pearsonDF.index
                    pearsonDF.index = range(len(pearsonDF))
                    pearsonDF.head()
                    topUsers=pearsonDF.sort_values(by='similarityIndex', ascending=False)[0:50]
                    print(topUsers.head())
                    topUsersRating=topUsers.merge(rating_df, left_on='TraineeID', right_on='TraineeID', how='inner')
                    topUsersRating.head()
                    topUsersRating['weightedRating'] = topUsersRating['similarityIndex']*topUsersRating['rating']
                    print(topUsersRating.head())
                    #Applies a sum to the topUsers after grouping it up by userId
                    tempTopUsersRating = topUsersRating.groupby('DocID').sum()[['similarityIndex','weightedRating']]
                    tempTopUsersRating.columns = ['sum_similarityIndex','sum_weightedRating']
                    print(tempTopUsersRating.head())
                    #Creates an empty dataframe
                    recommendation_df = pd.DataFrame()
                    
                    #Now we take the weighted average
                    recommendation_df['weighted average recommendation score'] = tempTopUsersRating['sum_weightedRating']/tempTopUsersRating['sum_similarityIndex']
                    recommendation_df['DocID'] = tempTopUsersRating.index
                    print(recommendation_df.head())
                    recommendation_df = recommendation_df.sort_values(by='weighted average recommendation score', ascending=False)
                    print(recommendation_df.head(5))
                    print(doc_df.loc[doc_df['DocID'].isin(recommendation_df.head(5)['DocID'].tolist())])   
        
                    
        
 
