library(jsonlite)
library(dplyr)
#read in the yelp given datasets
bus<- stream_in(file('D:\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_business.json'))
rev<- stream_in(file('D:\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_review.json'))
colnames(rev)
colnames(bus)
head(bus$categories)
#subset by just restaurants
restaurants<-bus[sapply(bus$categories,function(x){return("Restaurants" %in% x)}),]
restaurants$hours
#remove those restaurants that don't have data for hours
restaurants<-restaurants[-which(rowSums(is.na(restaurants$hours))==14),]
colSums(is.na(restaurants$attributes))
head(restaurants$attributes)
#remove those restaurants that don't have data for takeout
which(is.na(restaurants$attributes$`Take-out`))
res2<- restaurants[-which(is.na(restaurants$attributes$`Take-out`)),]
colSums(is.na(res2$attributes))
#remove those restaurants that don't have data for Delivery
which(is.na(res2$attributes$`Delivery`))
res3 <- res2[-which(is.na(res2$attributes$`Delivery`)),]
colSums(is.na(res3$attributes))
#remove those restaurants that don't have data for meals
which(rowSums(is.na(res3$attributes$`Good For`))>0)
res4 <- res3[-which(rowSums(is.na(res3$attributes$`Good For`))>0),]
colSums(is.na(res4$attributes))
#remove those restaurants that don't have data for price
which(is.na(res4$attributes$`Price Range`))
res5 <- res4[-which(is.na(res4$attributes$`Price Range`)),]
colSums(is.na(res5$attributes))
#remove those restaurants that don't have data for reservations
which(is.na(res5$attributes$`Takes Reservations`))
res6<- res5[-which(is.na(res5$attributes$`Takes Reservations`)),]
colSums(is.na(res6))
#remove those restaurants that are not open
res7 <- res6[-which(res6$open == FALSE),]
#Find the main category of each restaurant
res7$newCat = NA
for( i in 1:nrow(res7)){
  if(length(res7$categories[[i]])==1){
    print(res7$categories[[i]])
  } else{
    if(res7$categories[[i]][1]=="Restaurants"){
      if(res7$categories[[i]][2]=="Food"){
        if (length(res7$categories[[i]]>2)){
          res7$newCat[i] = res7$categories[[i]][3]
        }
      } else{
        res7$newCat[i] = res7$categories[[i]][2]
      }
      
    }else{
      if(res7$categories[[i]][1]!="Food"){
        res7$newCat[i] = res7$categories[[i]][1]
      } else{
        if(res7$categories[[i]][2] != "Restaurants"){
          res7$newCat[i] = res7$categories[[i]][2]
        } else{
          if(length(res7$categories[[i]])>2){
            res7$newCat[i] = res7$categories[[i]][3]
          }
        }
      }
    }
  }
  
}
head(res7$newCat)
res7$newCat = as.character(res7$newCat)
summary(res7$newCat)
res7 <- res7[-which(is.na(res7$newCat)),]
#put it all together
Restaurant.data<- cbind(res7$business_id,res7$full_address, res7$newCat,res7$city,res7$review_count,res7$name,res7$longitude,res7$latitude,res7$state,res7$stars)
colnames(Restaurant.data) <- paste(c('business_id','full_address','main_cat', 'city','review_count', 'name', 'longitude', 'latitude', 'state', 'stars'))
#find weekend and weekday hours
Restaurant.hours <- res7$hours
for(i in 1:ncol(Restaurant.hours)){
  close<-strptime(Restaurant.hours[[i]]$close, format = '%H:%M')
  open<- strptime(Restaurant.hours[[i]]$open, format = '%H:%M')
  diff<- difftime(close,open,units = "hours")
  needAdd <- which(diff<=0)
  diff[needAdd] = diff[needAdd] + 24
  diff[which(is.na(diff))] = 0
  Restaurant.hours[[i]]$numHours = diff
}
Restaurant.hours$WeekdayHours = Restaurant.hours$Monday$numHours+Restaurant.hours$Tuesday$numHours+Restaurant.hours$Wednesday$numHours+Restaurant.hours$Thursday$numHours+Restaurant.hours$Friday$numHours
Restaurant.hours$WeekendHours = Restaurant.hours$Saturday$numHours+Restaurant.hours$Sunday$numHours

#attributes
Restaurant.attributes <- cbind(res7$attributes$`Accepts Credit Cards`,res7$attributes$`Price Range`,res7$attributes$Delivery,res7$attributes$`Take-out`,res7$attributes$`Takes Reservations`)
ResAtNames <- c('CreditCard', 'Price', 'Delivery', 'Takeout', 'Reservations')
Restaurant.att <- NA
for(i in 1:5){
  
Restaurant.attributes[unlist(lapply(Restaurant.attributes[,i],is.null)),i]<-NA
Restaurant.att<-cbind(Restaurant.att,unlist(Restaurant.attributes[,i]))
}
Restaurant.att<- Restaurant.att[,-1]
colnames(Restaurant.att)<- paste(ResAtNames)
Restaurant.att
Restaurant.good<-res7$attributes$`Good For`
head(Restaurant.attributes)
head(Restaurant.good)
#new restaurant data
Restaurant.info<- cbind(Restaurant.data,Restaurant.hours$WeekdayHours,Restaurant.hours$WeekendHours,Restaurant.att, Restaurant.good)
write.csv(Restaurant.info, "Restaurant.csv")
summary(Restaurant.info)
##################
#find the median income, mean income, and population by zip code
medMean <- read.csv('MedianZIP-3.csv')

summary(restaurant$full_address)
#function to help find zip code
substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}
#find zip code by address
restaurant$zip <- as.numeric(substrRight(as.character(restaurant$full_address),5))
#remove those restaurants that didn't have zip codes
restaurant<- restaurant[-which(is.na(restaurant$zip)),]
#subset the reviews according to restaurants
reviews<-reviews[which(reviews$BusinessId %in% restaurant$business_id),]

colnames(restaurant)
#find the zip that's closest within the medMean
closezips<- c()
for(i in 1:nrow(restaurant)){
  closezips[i]<-medMean[which(abs(medMean$Zip-restaurant$Zip[i])==min(abs(medMean$Zip-restaurant$Zip[i]))),1]
}
summary(closezips)
restaurant<- cbind(restaurant,closezips)
restaurant<- merge(restaurant,medMean, by = "closezips", all.x= TRUE)
colnames(medMean)[1] <- paste("closezips")
#check for na's 
colSums(is.na(restaurant))
colnames(restaurant)
nazips=cbind(nazips,NA)
nazips<- cbind(nazips,n)
colnames(nazips)[2]<- paste("closezips")
#merge the restaurant with close zips
restaurant<- cbind(restaurant,closezips)
colnames(medMean)[1] <- paste("closezips")
restaurant<- merge(restaurant,medMean, by = "closezips", all.x= TRUE)
#make mean, median, and population numeric
restaurant$Mean <- as.numeric(gsub(",","", restaurant$Mean))
restaurant$Median <- as.numeric(gsub(",","", restaurant$Median))
restaurant$Pop <- as.numeric(gsub(",","", restaurant$Pop))
#subset reviews based on subsetted restaurants
colnames(rev)
Reviews<-rev[which(rev$business_id %in% Restaurant.info$business_id),]
colnames(Reviews)
Reviews[,1]
Review.info <- cbind(Reviews$user_id,Reviews$review_id,Reviews$stars,Reviews$date,Reviews$text,Reviews$business_id)
write.csv(Review.info, "Review.csv")
