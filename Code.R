#                                            #
#      *** HOW TO READ THE SCRIPT ***        #
#                                            #
#  This script contains the necessary code   #
#  for completing the questions. Code that  Â #
#  is delimited by ----- its used for check  #
#  result using data manipulation in R.      #


##############################################
#                     TASK 1                 #      
##############################################

# Libraries
library(redux)
library(dplyr)

# Create a connection to the local instance of REDIS
r <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))

# Import dataset 1: UserId that have received at least one email 
df_email = read.csv("emails_sent.csv") ; head(df_email, 8) 

# Import dataset 2: contains all the User IDs of the classifieds provider
#                   and a flag that indicates whether the user performed 
#                   a modification on his/her listing
df_modify = read.csv("modified_listings.csv") ; head(df_modify, 8)

# -------------------------------------------------------------------
# Total user that have received at least one email 
tot_email_users = unique(df_email$UserID); length(tot_email_users)
# EmailID are unique? yes
length(unique(df_email$EmailID))

# Check theÂ number users in second dataset
tot_users = unique(df_modify$UserID); length(tot_users)
# Check if each users has 3 months : True
nrow(df_modify)/3  == length(tot_users)
# -------------------------------------------------------------------

#-----#
# 1.1 #  How many users modified their listing on January?
#-----#  

# -> Loop + if | generate 1 key |  count

# Loop 
for(i in 1:nrow(df_modify)){
     # Conditions
     if((df_modify$ModifiedListing[i] == 1) & (df_modify$MonthID[i] == 1)) {
        # Set the key 
        r$SETBIT ("ModificationJanuary", df_modify$UserID[i], "1")
       
    }
}

# Calculate "1's" in the Key
result1 = r$BITCOUNT("ModificationJanuary"); result1

#-----#
# 1.2 # - How many users did NOT modify their listing on January?
#-----#

# -> "NOT" operator sets the bits that are 0 to 1 | count

# Bitwise operation 
r$BITOP("NOT","ModificationNot","ModificationJanuary")
# Count 
result2 = r$BITCOUNT("ModificationNot"); result2

# These numbers does not match the total number of users. Explained in the report.
isTRUE(result1 + result2 == tot_users)

#-----# - How many users received at least one e-mail per month  at least one e- mail in March)?
# 1.3 #   (at least one e- mail in January and at least one e-mail in February and
#-----#   at least one e- mail in March)?

#  -> Loop + conditions | generate 3 keys | logical operation AND | count

for(i in 1:nrow(df_email)){ 
  
  if (df_email$MonthID[i] == 1){
    
    r$SETBIT("EmailsJanuary", df_email$UserID[i] , "1")
  } 
  
  else if (df_email$MonthID[i] == 2){
    
    r$SETBIT("EmailsFebruary", df_email$UserID[i] , "1")
  
  } else {
    
    r$SETBIT("EmailsMarch", df_email$UserID[i] , "1")
  }
}

# Check results
r$BITCOUNT("EmailsJanuary")
r$BITCOUNT("EmailsFebruary")
r$BITCOUNT("EmailsMarch")

# Bitwise operation
r$BITOP("AND","Result3", c("EmailsJanuary","EmailsFebruary","EmailsMarch"))
# Count
result3 = r$BITCOUNT("Result3"); result3

#-----# 
# 1.4 # - How many users received an e-mail on January and March but NOT on February?
#-----#   

# -> Logical negation of the key "EmailsFebruary" | 
# -> logical operation AND with keys generated in question 3

r$BITOP("NOT", "NotFebruary", "EmailsFebruary")
r$BITCOUNT("NotFebruary") 

r$BITOP("AND", "Result4" , c(c("EmailsJanuary","NotFebruary","EmailsMarch")))
result4 = r$BITCOUNT("Result4") ; result4

#-----# 
# 1.5 # - How many users received an e-mail on January that they 
#-----#   did not open but they updated their listing anyway 

#  -> Merge df |  loop + conditions | generate 1 key |  then count 

# Merge the dataframe
alldata = merge(df_email, df_modify, by = "UserID")

# A. (supposing in the same month)
for(i in 1:nrow(alldata)) { 
   
  if((alldata$EmailOpened[i] == 0) & 
     (alldata$MonthID.x[i] == 1) &
     (alldata$ModifiedListing[i] == 1) &
     (alldata$MonthID.y[i] == 1 )) {
    
         r$SETBIT("EmailsNotOpenedJanuary", alldata$UserID[i] , "1")
 
     } 
}

result5 = r$BITCOUNT("EmailsNotOpenedJanuary") ; result5

#-------------------------------------
# Check with dplyr
alldata_5 = alldata %>% 
  filter(EmailOpened == 0 & MonthID.x == 1 & MonthID.y == 1 & ModifiedListing == 1 )

length(unique(alldata_5$UserID))

#-------------------------------------
# B. ( modified listing in any month)
for(i in 1:nrow(alldata)) { 
  
  if((alldata$EmailOpened[i] == 0) & 
     (alldata$MonthID.x[i] == 1) &
     (alldata$ModifiedListing[i] == 1)) {
    
    r$SETBIT("EmailsNotOpenedJanuary_full", alldata$UserID[i] , "1")
    
  } 
}

r$BITCOUNT("EmailsNotOpenedJanuary_full") 
#-----------------------------------------

#-----# 
# 1.6 # 
#-----#
# How many users received an e-mail on January that they did not open
# but they updated their listing anyway on January OR they received an
# e-mail on February that they did not open but they updated their listing
# anyway on February OR they received an e-mail on March that they did not 
# open but they updated their listing anyway on March?

#  -> Merge df |  loop + conditions | generate 2 key |
#  -> logical AND operation with the 2 keys generate in q       question 5 | then count


for(i in 1:nrow(alldata)) { 
  
  if((alldata$EmailOpened[i] == 0) & 
     (alldata$MonthID.x[i] == 2) &
     (alldata$ModifiedListing[i] == 1) &
     (alldata$MonthID.y[i] == 2 )) {
    
    r$SETBIT("EmailsNotOpenedFebruary", alldata$UserID[i] , "1")
    
  } else if ((alldata$EmailOpened[i] == 0) & 
    (alldata$MonthID.x[i] == 3) &
    (alldata$ModifiedListing[i] == 1) &
    (alldata$MonthID.y[i] == 3 )) {
    
    r$SETBIT("EmailsNotOpenedMarch", alldata$UserID[i] , "1")
    
  }
}

# Quick look  
r$BITCOUNT("EmailsNotOpenedFebruary")
r$BITCOUNT("EmailsNotOpenedMarch")

# Bitwise operation  + result 
r$BITOP("OR", "Result6", c("EmailsNotOpenedJanuary",
                           "EmailsNotOpenedFebruary",
                           "EmailsNotOpenedMarch")) 
result6 = r$BITCOUNT("Result6"); result6

#------------------------
# Check
alldata_jan = alldata %>% 
  filter(EmailOpened  == 0 & MonthID.x == 1 & MonthID.y == 1 & ModifiedListing == 1 )

alldata_feb = alldata %>% 
  filter(EmailOpened  == 0 & MonthID.x == 2 & MonthID.y == 2 & ModifiedListing == 1 )

alldata_mar = alldata %>% 
  filter(EmailOpened  == 0 & MonthID.x == 3 & MonthID.y == 3 & ModifiedListing == 1 )

jan4 = length(unique(alldata_jan$UserID)); jan4
feb4 = length(unique(alldata_feb$UserID)); feb4
mar4 = length(unique(alldata_mar$UserID)); mar4
#-----------------------

#-----# 
# 1.7 # 
#-----#
# Does it make any sense to keep sending e-mails with
# recommendations to sellers? Does this strategy really work?
# How would you describe this in terms a business person would understand?

# We can have 4 different scenarios (we analyze the behaviour month by month)
# 1) Email Read , Modification  (positive impact)
# 2) Email Non Read, Non Modification
# 3) Email  Read,  Non Modification 
# 4) Email Non Read,  Modification  (already calculated in question6 , stored with #4)

# -> Intuitively the number in "1" should be >> than "3" if the strategy works. 
# -> Moreover, its useful comparing the total email read that lead to a modification 
# -> in the listing, and the total email sent without any positive impact 

# 1. Read the email and changed their listing in each month. 

for(i in 1:nrow(alldata)){
  
  if ((alldata$EmailOpened[i]==1)
      &(alldata$MonthID.x[i]==1)
      &(alldata$MonthID.y[i]==1)
      &(alldata$ModifiedListing[i]==1)){
    
    r$SETBIT("Open_Mod_Jan", alldata$UserID[i],"1")
    
  }else if ((alldata$EmailOpened[i]==1)
            &(alldata$MonthID.x[i]==2)
            &(alldata$MonthID.y[i]==2)
            &(alldata$ModifiedListing[i]==1)){
    
    r$SETBIT("Open_Mod_Feb", alldata$UserID[i],"1")
    
  }else if ((alldata$EmailOpened[i]==1)
            &(alldata$MonthID.x[i]==3)
            &(alldata$MonthID.y[i]==3)
            &(alldata$ModifiedListing[i]==1)){
    
    r$SETBIT("Open_Mod_Mar", alldata$UserID[i],"1")
  }
}

r$BITCOUNT("Open_Mod_Jan")
r$BITCOUNT("Open_Mod_Feb")
r$BITCOUNT("Open_Mod_Mar")

#--------------------------
# Check 
alldata_jan1 = alldata %>% 
  filter(EmailOpened  == 1 & MonthID.x == 1 & MonthID.y == 1 & ModifiedListing == 1 )

alldata_feb1 = alldata %>% 
  filter(EmailOpened  == 1 & MonthID.x == 2 & MonthID.y == 2 & ModifiedListing == 1 )

alldata_mar1 = alldata %>% 
  filter(EmailOpened  == 1 & MonthID.x == 3 & MonthID.y == 3 & ModifiedListing == 1 )

jan1 = length(unique(alldata_jan1$UserID)); jan1
feb1 = length(unique(alldata_feb1$UserID)); feb1
mar1 = length(unique(alldata_mar1$UserID)); mar1
#-----------------------------

# 2.Not read email, not modification.

for(i in 1:nrow(alldata)){

  if ((alldata$EmailOpened[i]==0)
      &(alldata$MonthID.x[i]==1)
      &(alldata$MonthID.y[i]==1)
      &(alldata$ModifiedListing[i]==0)){

    r$SETBIT("Not_Open_Not_Mod_Jan", alldata$UserID[i],"1")

  }else if ((alldata$EmailOpened[i]==0)
            &(alldata$MonthID.x[i]==2)
            &(alldata$MonthID.y[i]==2)
            &(alldata$ModifiedListing[i]==0)){

    r$SETBIT("Not_Open_Not_Mod_Feb", alldata$UserID[i],"1")

  }else if ((alldata$EmailOpened[i]==0)
            &(alldata$MonthID.x[i]==3)
            &(alldata$MonthID.y[i]==3)
            &(alldata$ModifiedListing[i]==0)){

    r$SETBIT("Not_Open_Not_Mod_Mar", alldata$UserID[i],"1")
  }
}

jan2 = r$BITCOUNT("Not_Open_Not_Mod_Jan"); jan2
feb2 = r$BITCOUNT("Not_Open_Not_Mod_Feb"); feb2
mar2 = r$BITCOUNT("Not_Open_Not_Mod_Mar"); mar2


# Check 
alldata_jan2 = alldata %>% 
  filter(EmailOpened  == 0 & MonthID.x == 1 & MonthID.y == 1 & ModifiedListing == 0 )

alldata_feb2 = alldata %>% 
  filter(EmailOpened  == 0 & MonthID.x == 2 & MonthID.y == 2 & ModifiedListing == 0 )

alldata_mar2 = alldata %>% 
  filter(EmailOpened  == 0 & MonthID.x == 3 & MonthID.y == 3 & ModifiedListing == 0 )

jan2 = length(unique(alldata_jan2$UserID)); jan2
feb2 = length(unique(alldata_feb2$UserID)); feb2
mar2 = length(unique(alldata_mar2$UserID)); mar2


# 3. Email Read, Not Modification 

for(i in 1:nrow(alldata)){
  
  if ((alldata$EmailOpened[i]==1)
      &(alldata$MonthID.x[i]==1)
      &(alldata$MonthID.y[i]==1)
      &(alldata$ModifiedListing[i]==0)){
    
    r$SETBIT("Open_Not_Mod_Jan", alldata$UserID[i],"1")
    
  }else if ((alldata$EmailOpened[i]==1)
            &(alldata$MonthID.x[i]==2)
            &(alldata$MonthID.y[i]==2)
            &(alldata$ModifiedListing[i]==0)){
    
    r$SETBIT("Open_Not_Mod_Feb", alldata$UserID[i],"1")
    
  }else if ((alldata$EmailOpened[i]==1)
            &(alldata$MonthID.x[i]==3)
            &(alldata$MonthID.y[i]==3)
            &(alldata$ModifiedListing[i]==0)){
    
    r$SETBIT("Open_Not_Mod_Mar", alldata$UserID[i],"1")
  }
}

jan3 = r$BITCOUNT("Open_Not_Mod_Jan"); jan3
feb3 = r$BITCOUNT("Open_Not_Mod_Feb"); feb3
mar3 = r$BITCOUNT("Open_Not_Mod_Mar"); mar3


# Check 
alldata_jan3 = alldata %>% 
  filter(EmailOpened  == 1 & MonthID.x == 1 & MonthID.y == 1 & ModifiedListing == 0 )

alldata_feb3 = alldata %>% 
  filter(EmailOpened  == 1 & MonthID.x == 2 & MonthID.y == 2 & ModifiedListing == 0 )

alldata_mar3 = alldata %>% 
  filter(EmailOpened  == 1 & MonthID.x == 3 & MonthID.y == 3 & ModifiedListing == 0 )

jan3 = length(unique(alldata_jan3$UserID)); jan3
feb3 = length(unique(alldata_feb3$UserID)); feb3
mar3 = length(unique(alldata_mar3$UserID)); mar3


# PLOT: create a df to plot
months = c(rep("January",4),
           rep("February",4),
           rep("March",4))

conditions = rep(c("EmailRead_Modification",
                   "EmailNotRead_NotModification",
                   "EmailRead_NotModification",
                   "EmailNotRead_Modification"),3) # condtion 
values = c(jan1,jan2,jan3,jan4,
           feb1,feb2,feb3,mar4,
           mar1,mar2,mar3,mar4)

df_plot=data.frame(months,conditions,values)


# Grouped
library(ggplot2)
ggplot(df_plot, aes(fill=conditions, y=values, x=months)) + 
  geom_bar(position="dodge", stat="identity") +
  scale_x_discrete(limits=c("January","February","March")) + 
  scale_fill_manual("conditions", values = c("EmailRead_Modification" = "darkgreen", 
                                         "EmailNotRead_NotModification" = "lightyellow",
                                         "EmailRead_NotModification" = "red",
                                         "EmailNotRead_Modification" = "lightsalmon"))+
  ggtitle("Strategy scenarios")+
  theme(plot.title = element_text(size = 20, face = "bold"))
  

# Compare the email that made a positive impact with the total emails sent 

# Globally
positive = sum(jan1,feb1,mar1);positive
totalsent = sum(jan1,feb1,mar1,
                jan2,feb2,mar2,
                jan3,feb3,mar3,
                jan4,feb4,mar4)
effectiveness = (positive/totalsent) * 100; effectiveness

# Locally 
effectiveness_jan = (jan1 / sum(jan1,jan2,jan3,jan4)) *100; effectiveness_jan
effectiveness_feb = (feb1 / sum(jan1,jan2,jan3,jan4)) *100; effectiveness_feb
effectiveness_mar = (mar1 / sum(jan1,jan2,jan3,jan4)) *100; effectiveness_mar

# Plot monthly effectiveness
months = c("January","February","March")
values = c(effectiveness_jan,effectiveness_feb,effectiveness_mar)
dfplot2 = data.frame(months, values)

ggplot(dfplot2, aes(x = months, y = values, fill = months))+
    geom_bar(position="dodge", stat="identity")+
    scale_x_discrete(limits=c("January","February","March")) +
    ggtitle("Monthly effectiveness")+
    theme(plot.title = element_text(size = 20, face = "bold")) +
    ylab("% Percentage")

  
#-------------- DON'T RUN ------------
# Check manually uniqueness of the key
r$FLUSHALL()
for(i in 1:nrow(alldata[1:12,])){
  
  if ((alldata$EmailOpened[i]==0)
      &(alldata$MonthID.x[i]==2)
      &(alldata$MonthID.y[i]==2)
      &(alldata$ModifiedListing[i]==0)){
    
    r$SETBIT("try", alldata$UserID[i],"1")}}
r$BITCOUNT("try")
#-------------------------------------------------------

#######################################################
#                     TASK 2                          #
#######################################################
      
## * Libraries * 
library(jsonlite)
library(mongolite)
library(dplyr)

## * Reading paths * 

# Import a vector containing all the paths 
path = dir(path = "/Users/University/projects/BigDataSystem/BIKES", pattern = "\\.json$", 
           full.names = TRUE, recursive = TRUE)

# Put as a list
pathlist = as.list(path) ; length(list)
head(pathlist)

# Create a file txt 
capture.output(list, file = "List.txt")

#--------------------------------
#---- * Mongolite approach * ----
#--------------------------------

# Open connection 
m <- mongo(collection = "fulldf",  db = "Scrape_Bikes", url = "mongodb://localhost")

# Read and insert each Json files in the collection 
for(i in  1:length(pathlist)){
  m$insert(fromJSON(readLines(pathlist[[i]], warn = F)))
}

# Create a big dataframe (is composed by other dataframe (list in a list more or less))
newdf = m$find("{}")

# Look the name of the columns 
names(newdf); nrow(newdf)

# Take dataframes 
query_df = newdf[,"query"]; names(query_df)
ad_data_df = newdf[,"ad_data"]; names(ad_data_df)
ad_seller_df = newdf[,"ad_seller"]; names(ad_seller_df)
meta_data_df  = newdf[,"metadata"]; names(meta_data_df)
# Take Lists
list_df = newdf[,c("title","ad_id","extras","description")]

# Note there are two columns with the same name (type) :
# MongoDB doesn't allow to have two keys with the same name
# (they are in query_df, meta_data_df)
colnames(meta_data_df) = c("type2","brand","model")

# Build the dataframe
trial = cbind(query_df, ad_data_df,ad_seller_df,meta_data_df,list_df)
# Dplyr function to understand the dataframe 
glimpse(trial)

# ---- ! Try some query ! -----
t <- mongo(collection = "Cleaned",  db = "Scrape_Bikes", url = "mongodb://localhost")
t$insert(trial)

# How many bikes are there for sale?
pipe = '[{"$group" : {"_id" : "$type", "count": {"$sum":1} }}]'
count = t$aggregate(pipeline = pipe)
count
# Find electric fuel type bikes
# -> Note that the framework to work is not optimized 
# -> You retrieve all the columns! It's better to query using field 
# -> paramater as illustrated in the next steps 
electric <- t$find('{"Fuel type" : "Electric" }')
electric

#  --> In that way it works =)

#-----------------------
#---- * Cleaning  * ----
#-----------------------

#  --------------- ! Test cleaning actions use small df ! ---------------
#  ->        if you need to perform cleaning actions without loading 
#  ->        again the full dataset, just select all the lines, then 
#  ->                 (command + shift + c ) to delete 

# # Take a small subset
# newdf2 = newdf[1:100,]
# 
# # * PRICE *
# # Remove â¬ symbol
# newdf2$ad_data$Price <- gsub("â¬", "",newdf2$ad_data$Price)
# # You may need to escape the . which is a special character that means "any character"
# newdf2$ad_data$Price <- gsub("\\.", "",newdf2$ad_data$Price)
# # Transform in numeric
# newdf2$ad_data$Price = as.numeric(newdf2$ad_data$Price)
# 
# #Â * MILEAGE *
# # Remove Km
# newdf2$ad_data$Mileage <- gsub(" km", "",newdf2$ad_data$Mileage)
# # Remove commas
# newdf2$ad_data$Mileage <- gsub("," , "",newdf2$ad_data$Mileage)
# # Transform in numeric
# newdf2$ad_data$Mileage = as.numeric(newdf2$ad_data$Mileage)
# 
# # * REGISTRATION * (take care just on the year)
# # Remove all before and up to ":":
# newdf2$ad_data$Registration = gsub(".*/","",newdf2$ad_data$Registration)
# # Transform in numeric
# newdf2$ad_data$Registration = as.numeric(newdf2$ad_data$Registration)

# * EXTRAS / DESCRIPTION * 
# Probably need cleaning in further steps 

# ->  When its all clean with the small dataset we can now perform the cleaning with the big one! 
# -----------------------------------------------------------------------------------------------

# * 1.PRICE * 
# Remove â¬ symbol  
trial$Price <- gsub("â¬", "",trial$Price)
# You may need to escape the . which is a special character that means "any character"
trial$Price <- gsub("\\.", "",trial$Price)
# Transform in numeric
trial$Price = as.numeric(trial$Price)

# * 2.MILEAGE *
# Remove Km 
trial$Mileage <- gsub(" km", "",trial$Mileage)
# Remove commas
trial$Mileage <- gsub("," , "",trial$Mileage)
# Transform in numeric 
trial$Mileage = as.numeric(trial$Mileage)

# * 3.REGISTRATION * (take care just on the year)
# Remove all before and up to ":":
trial$Registration = gsub(".*/","",trial$Registration)
# Transform in numeric
trial$Registration = as.numeric(trial$Registration)

#-----------------------
#---- * Questions * ----
#-----------------------

# Mongo Connection
t$drop()
t <- mongo(collection = "Cleaned",  db = "Scrape_Bikes", url = "mongodb://localhost")
t$insert(trial)

## -- Debugging queries -- 
error = '{"type": { $regex : "CAR"}}'
jsonlite::fromJSON(error) # Forgot the "" !

#-----#
# 2.2 # - How many bikes are there for sale?
#-----#

# Query 
pipe = '[{"$group" : {"_id" : "$type2", "count": {"$sum":1} }}]'
number_bikes = t$aggregate(pipeline = pipe); number_bikes
# Check in R : OK!
nrow(trial)  == number_bikes[2]

#-----# - Average Price of a motorcycle?
# 2.3 # - Number of listing used to calculate this avg?
#-----# - Is this number the same as 2.2 ?

# Query 1
avg_price = t$aggregate(
                       '[{"$group" : {"_id" : "AveragePrice", "avg_price":{"$avg": "$Price"}}}]'
                       ) 
avg_price
# Check in R : OK!
mean(trial$Price, na.rm = T) == avg_price[2]

# Query 2 + Time comparison 
slow  = system.time(t$find('{ "Price" : { "$gte" : 0 } }'))

total_listing_price_fast = t$find(
                                 query  = '{ "Price" : { "$gte" : 0 } }',
                                 fields = '{ "Price" : true }'
                                 )
fast = system.time(t$find(
                         query  = '{ "Price" : { "$gte" : 0 } }',
                         fields = '{ "Price" : true }'
                         ))
slow;fast

# Take the difference 
difference = nrow(newdf) - nrow(total_listing_price_fast); difference
# Number used to calculate : -> the number is not the same because some listing "askforprice" 
number_used_to_calculate = number_bikes[2]- difference; number_used_to_calculate

#-------- Removing symbolic price --------

# List of unique prices
sort(unique(trial$Price))[1:100]

# How many are symbolic? The threshold is arbitrary
# -> Generally looking the website scraped and considering the market, 
# -> below 70 euro, they are all symbolic or replacement parts. 
# -> Between 70-200 euro we expect bikes that are crashed or very old. 
symbolic = t$find(
                 query = '{"Price" : {"$lt" : 70}}',
                 fields = '{"Price" : "true", "url" : "true"}'
                 )
symbolic; nrow(symbolic)

# Query 1
avg_price = t$aggregate('[ 
                         {"$match" : {"Price" : {"$gt" : 70}}},
                         {"$group" : {"_id" : "AveragePrice", "avg_price":{"$avg": "$Price"}}}
                         ]') 
avg_price


#-----#
# 2.4 # - What is the maximum and minimum price of a motorcycle currently available in the market
#-----#

# Query 1
max = t$find(
  query  = '{"type2" : "Bikes"}', sort = '{"Price": -1}',
  fields = '{"type2" : "true" , "Price" : "true"}',
  limit = 1
)
print(max)
# Check in R : OK!
sort(trial$Price,decreasing =  T)[1] == max$Price

# Query 2
min = t$find(
  query  = '{"type2" : "Bikes", "Price" : {"$gte" : 0}}', sort = '{"Price": 1}',
  fields = '{"type2" : "true" , "Price" : "true"}',
  limit = 1
)
print(min)
# Check in R : OK!
sort(trial$Price,decreasing =  F)[1] == min$Price

#-----#
# 2.5 # - # How many listings have a price that is identified as "Negotiable"?
#-----#

# To find if this string is stored in the dataframe.
value = "Negotiable"
# Let's check in which column we found this value 
l <- sapply(colnames(trial), function(x) grep(value, trial[,x]))
l
# Ok! The word negotiable is stored in the model columnn! 

# Check in R how many listing are identified as negotiable
negotiable = t$find(
  query = '{"model": { "$regex" : "Negotiable", "$options" : "i" }}',
  fields = '{"model": "true", "Price":"true"}'
)
tot = nrow(negotiable); tot

# Mongo classic query 

t$aggregate('[
  {
    "$match": {
      "model" : { "$regex" : "Negotiable", "$options" : "i"}
    }
  },
  {
    "$group": {
      "_id": "null",
      "negotiable_ads": {
        "$sum": 1
      }
    }
  }
  ]')


#-----#
# 2.6 # - For each Brand, what percentage of its listings is listed as negotiable?
#-----#

# - Percentange is not implememented in MongoDb
# - It is necessary our own code to calculate it!
# - Note: this query has been first solved in robo3T,
# -       then we used R to display in a better way the results. 
# 
# !!!! We don't obtained a correct result using Mongo Query,
#      due to a difficult (for us) sequence of aggregations. 
#      But we exploit the data manipulation in R to get the result,
#      in a less time consuming method. 

total_listing_brand = t$aggregate ('[
  {
    "$group" : {
      "_id" : {"brand" : "$brand"}, 
      "count": {
        "$sum" : 1
      }
    }
  }]')

# Have a look 
glimpse(total_listing_brand)
# Unnest the dataframe 
df1 = cbind(total_listing_brand$`_id`,total_listing_brand$count)
# Have a look again 
glimpse(df1)

# Query ! Doesn't calculate properly the percentage, 
#         because we filtered with $match at the first 
#         stage the documents that have negotiable in "model".
#   ->    We can still exploiting the fastness of Mongodb queries,
#         to further subset what we want to take. 

pipe = '[ 
       { 
        "$match": {
                 "model" : { "$regex" : "Negotiable", "$options" : "i"}
        }
      },
       { "$group": { "_id": {"brand":  "$brand"}, 
                            "count": { "$sum": 1 }}},    
       { "$project": { 
            "count": 1, 
              "percentage": { 
                "$concat": [ { "$substr": [ { "$multiply": 
                 [ { "$divide": [ {"$literal": 29701 }, "$count"] }, 100 ] }, 0,2 ] }, "", "%" ]}
   }
  }
  ]'


# Result 
number_negotiable_brand = t$aggregate(pipeline = pipe); number_negotiable_brand
# Have a look 
glimpse(number_negotiable_brand)
# Unnest the dataframe
df2 = cbind(number_negotiable_brand$`_id`,number_negotiable_brand$count)
# Have a look again 
glimpse(df2)

# Merge df1, df2 (to compare and get percentage)
df2$total_listing_brand = df1[match(unlist(df2$brand),unlist(df1$brand)),2]
# Add the percentange
df2$percentage = paste(round((df2$`number_negotiable_brand$count` / df2$total_listing_brand)*100,1),"%")

# Check with result from 2.5 if everything is ok! yes
sum(number_negotiable_brand$count) == nrow(negotiable)

# Export to latex
install.packages("kableExtra")
library(kableExtra)
df2 %>% 
  kable(format ="latex")

#-----#
# 2.7 # - What is the motorcycle brand with the highest average price?
#-----#

pipe = '[{"$group" : 
                   {"_id" : "$brand", "avg_price": {"$avg":"$Price"} }},

         {"$sort"  :
                   {"avg_price" : -1}},

         {"$limit" : 1}]'


brand_highest_price = t$aggregate(pipeline = pipe);brand_highest_price

#-----#
# 2.8 # - What are the TOP 10 models with the highest average age? (Round age by one decimal number)
#-----#

# Query:
# -> We increase the $limit operator to 15 in order not exclude possible models with the same age.
# -> We round adding 0.5 to each average 
pipe = '[{"$group" : 
            {"_id" : "$model", "avg_age" : {"$avg" : {"$subtract":[2019, "$Registration"]}},
                                "count" : {"$sum": 1}}},
         {"$sort"  :
                    {"avg_age" : -1}},
         { "$project": { 
               "avg_age": 1, 
                "count": 1,
                "round": { 
                 "$divide": [ 
                   { "$trunc":  { "$add": 
                    [ { "$multiply": [ "$avg_age", 10]}, 0.5] }},10]}}},
         {"$limit" : 15}]'

top10_age = t$aggregate(pipeline = pipe); top10_age


#-----#
# 2.9 # - How many bikes have âABSâ as an extra?
#-----#

pipe = '[{"$match" : 
                    {"extras" : { "$regex" : "ABS", "$options" : "i" }}},
         {"$group" :
                    { "_id": "null",  "count": { "$sum": 1 } } }]'

abs = t$aggregate(pipeline = pipe); abs


#-----#
# 2.10 # - What is the average Mileage of bikes that have âABSâ AND âLed lightsâ as an extra?
#-----#

pipe = '[
  {
    "$match": { 
      "$and": [{ "extras" : { "$regex" : "ABS", "$options" : "i"}}, 
               { "extras" : { "$regex" : "Led lights", "$options" : "i"}}]
    }
  },
  {
    "$group": {
      "_id": "null",
      "avg_mileage": {
        "$sum": 1
      }
    }
  }
  ]'

avg_mileage_extras = t$aggregate(pipeline = pipe); avg_mileage_extras


#-----#
# 2.11 #  What are the TOP 3 colors per bike category?
#-----#

pipe = '[{"$group" :
                   { "_id": { "cat": "$Category", "col": "$Color"} ,
                                         "count": { "$sum": 1} } },
         {"$sort" : {"count": -1}},
         {"$group": 
                   {"_id" : "$_id.cat" , "bestcolor_count":
                                          {"$push": {"color" :"$_id.col", "count" : "$count"}}}},
         {"$project":
                    {"bestcolor_count":{"$slice":["$bestcolor_count", 3]}}}]'



jsonlite::fromJSON(pipe)

color = t$aggregate(pipeline = pipe); color

# Export to latex 
library(Hmisc)
latex(color, file="")   

#-----#
# 2.12 #  BEST DEALS? 
#-----#

# First hint : AGE / PRICE / EXTRAS / MODEL (negotiable) /DEALER PAGE (Better than website)
# / City (in greek problem) / kteo (technical control) / times clicked / Previous Owner / 
# Liter 100kms / Fuel type / Mileage / Price / Category 


# Unique models
unique(unlist(trial$model))


# Let's see the unique extras and read it  
# -> Price without VAT is a negative extra. 
# -> It's difficult to figure out how much an extra impact on a best deal 
# -> This info could be used as a final step 
unique(unlist(trial$extras))

# Let's see how wide is the market: A lot of cities. 
# -> We don't proceed including city/distances. 
unique(unlist(trial$City))

# Transmission? Which values can it take?
# -> Not very useful, it's an implicit characteristic of the model 
unique(unlist(trial$Transmission))

# Type2? Title?
# -> We stores 2 keys with the same exact information. Waste of space we can drop it 
# -> Repetition of infos
unique(unlist(trial$type2))


t$insert(trial)
# Let's delete some unused column  (they are - all = in each row
#                                            - all != in each row)
glimpse(trial)
list_drop = c("type","last_processed","type2", "ad_id")
trial_light  = trial[,!names(trial) %in% list_drop]
# Create a new column Age
trial_light = mutate(trial_light, age = 2019 - trial$Registration)

# Set NULL values in some fields = NA
trial_light$`Previous owners`[sapply(trial_light$`Previous owners`, is.null)] <- "NA"
trial_light$Dealer[sapply(trial_light$Dealer, is.null)] <- "NA"

# Let's calculate new variables
avgPricebyBrand = t$aggregate ('[
  {
    "$group" : {
      "_id" : {"brand" : "$brand"}, 
      "avg_price": {
        "$avg" : "$Price"
      }
    }
  }]')

# We append this new column 
trial_light$avgPricebyBrand = avgPricebyBrand[match(unlist(trial_light$brand), avgPricebyBrand$`_id`[,1]),2]

# Let's calculate new variables
avgPricebyCategory = t$aggregate ('[
  {
    "$group" : {
      "_id" : {"category" : "$Category"}, 
      "avg_price": {
        "$avg" : "$Price"
      }
    }
  }]')

# We append this new column 
trial_light$avgPricebyCategory = avgPricebyCategory[match(unlist(trial_light$Category), avgPricebyCategory$`_id`[,1]),2]

# New columns

avgMileagebyBrand = t$aggregate('[
  { "$group" : {
                 "_id" : {"brand" : "$brand"}, 
      "avg_mil": {
                "$avg" : "$Mileage"
      }
    }
  }]')

# We append this new column 
trial_light$avgMileagebyBrand = avgMileagebyBrand[match(unlist(trial_light$brand), avgMileagebyBrand$`_id`[,1]),2]

# Let's drop the previous collection and insert a new one, 
# that include some new variables  
t$drop()
# Insert new df 
t$insert(trial_light)

pipe = '[
    {"$project": { "Classified number" : 1, 
                   "url" : 1 ,
                   "age" : 1,
                   "Price": 1,
                   "Previous owners" : 1,
                   "Dealer" : 1,
                   "Category" : 1,
                   "low_price_brand":   {"$cmp": ["$Price","$avgPricebyBrand"]} ,
                   "low_mileage": {"$cmp": ["$Mileage", "$avgMileagebyBrand"]},
                   "low_price_cat" : {"$cmp": ["$Price", "$avgPricebyCategory"]}}},
    {"$match": {
        "$and":[
                {"low_mileage":{"$lt":0}},
                {"low_price_cat":{"$lt": 0}},
                {"age" :{"$lt" :5}},
                {"Price" : {"$gte" : 250}},
                {"Dealer": {"$exists":true, "$ne" : "NA"}}
                ]
     }
    }
  ]'

jsonlite::fromJSON(pipe)
a = t$aggregate(pipeline = pipe)


#------------------------------------------
#---- * INSERT DOCUMENTS IN MONGO DB * ----
#------------------------------------------

# This will insert directly in MongoDB.
# Pro : you can work with ROBOMONGO directly
# Cons: you cannot have a confirmation of results using R data manipulation packages 
#       and you cannot build visualization .... 

m$drop()
m <- mongo(collection = "Cleaned3T",  db = "Scrape_Bikes", url = "mongodb://localhost")

df <- {}
json_data <- {}

for(i in 1:length(pathlist)){
  
  # Read json files 1by1
  df[[i]] <- fromJSON(readLines(pathlist[[i]], warn = F))
  
  # Clean Price : Remove "â¬", then "."
  df[[i]]$ad_data$Price = gsub("â¬", "",  df[[i]]$ad_data$Price)
  df[[i]]$ad_data$Price = gsub("\\.", "",df[[i]]$ad_data$Price)
  df[[i]]$ad_data$Price = as.numeric(df[[i]]$ad_data$Price)
  
  # Clean Mileage : Remove "km", then "," 
  df[[i]]$ad_data$Mileage = gsub(" km","",df[[i]]$ad_data$Mileage)
  df[[i]]$ad_data$Mileage = gsub(",","",df[[i]]$ad_data$Mileage)
  df[[i]]$ad_data$Mileage = as.numeric(df[[i]]$ad_data$Mileage)
  
  # Clean Registration : Remove all digits before and up to / 
  df[[i]]$ad_data$Registration = as.numeric(gsub(".*/","", df[[i]]$ad_data$Registration))
  
  # Convert into json file 
  json_data[[i]] <- toJSON(df[[i]], auto_unbox = TRUE)
  
  # Insert in the mongo collection
  m$insert(json_data[[i]])
}


library(dplyr)
glimpse(df)

#-----------------------------------------------
