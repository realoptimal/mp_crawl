library('RMySQL')
library('Hmisc')
library('plyr')

# Establish the connection to the database
conn <- dbConnect(MySQL(), host='localhost', dbname='menupages', user='crawler')

venue_tbl_qry <- "SELECT * FROM venue"  # example
venue_tbl_res <- dbSendQuery(conn, venue_tbl_qry)
venue_tbl <- fetch(venue_tbl_res, n = -1)
dbClearResult(venue_tbl_res)

neighborhoods <- as.factor(venue_tbl$neighborhood)

detail_tbl_qry <- "SELECT * FROM detail"  # example
detail_tbl_res <- dbSendQuery(conn, detail_tbl_qry)
detail_tbl <- fetch(detail_tbl_res, n = -1)
dbClearResult(detail_tbl_res)

cuisines <- as.factor(detail_tbl$cuisine)

rating_tbl_qry <- "SELECT * FROM rating"  # example
rating_tbl_res <- dbSendQuery(conn, rating_tbl_qry)
rating_tbl <- fetch(rating_tbl_res, n = -1)
dbClearResult(rating_tbl_res)

vens.clean <- venue_tbl[grep("/menu", venue_tbl$url, invert=TRUE), ]
vens.clean.nodup <- vens.clean[!duplicated(vens.clean$name), ]

details.clean <- detail_tbl[!duplicated(detail_tbl$name), ]
m <- merge(vens.clean.nodup, details.clean, by="name")
ratings.clean <- rating_tbl[!duplicated(rating_tbl$name), ] 
m.full <- merge(m, ratings.clean, by="name")

DF.full <- with(m.full, data.frame(name, zip_code, area, neighborhood, cuisine, meals, features, count, average, food, value, service, atmosphere))
DF <- DF.full[DF.full$count != 0, ]

# Some quick search functions for the features
venues_featuring <- function (x) { 
	i <- grep(x, DF$features); 
	data.frame(cbind(name=DF$name[i], num=table(DF$name[i]), features=I(DF$features[i])), stringsAsFactors=FALSE, row.names=NULL) };

venues_not_featuring <- function (x) { 
	i <- grep(x, DF$features, invert=TRUE); 
	data.frame(cbind(name=DF$name[i], num=table(DF$name[i]), features=I(DF$features[i])), stringsAsFactors=FALSE, row.names=NULL) };

# Some interesting subsets
cash_only_i = grep("Accepts Credit Cards", DF$features, invert=TRUE)
csn.area.co <- DF[cash_only_i , c('name', 'area', 'cuisine', 'count', 'average', 'food', 'service', 'value', 'atmosphere')]
# order by area then by cuisine
csn.area.co <- csn.area.co[do.call(order, csn.area.co[, 2:3]), ]

wheelchair_i = grep("Wheelchair", DF$features)
csn.area.wcf <- DF[wheelchair_i , c('name', 'area', 'cuisine', 'count', 'average', 'food', 'service', 'value', 'atmosphere')]
# order by area then by cuisine
csn.area.wcf <- csn.area.wcf[do.call(order, csn.area.wcf[, 2:3]), ]

happy_hour_i = grep("Happy Hour", DF$features)
csn.area.hh <- DF[happy_hour_i , c('name', 'area', 'cuisine', 'count', 'average', 'food', 'service', 'value', 'atmosphere')]
# order by area then by cuisine
csn.area.hh <- csn.area.hh[do.call(order, csn.area.hh[, 2:3]), ]

csn.area.cntgtfive <- DF[DF$count > 5 , c('name', 'area', 'cuisine', 'count', 'average', 'food', 'service', 'value', 'atmosphere')]
# order by area then by cuisine
csn.area.cntgtfive <- csn.area.cntgtfive[do.call(order, csn.area.cntgtfive[, 2:3]), ]

# Predict food rating base on other ratings, cuisine, type, and area
mdl.df <- csn.area.cntgtfive; # One constraint is that we should only include stat rel. figures

# Starting simple, regress food rating against cuisine by area
mdl.fd.csn.by.area <- with(mdl.df, by(mdl.df, area, function (x) lm(food ~ cuisine, x)))

# Aggregate average food rating by cuisine type;
aggregate(food ~ cuisine, data=mdl.df, mean)


require(graphics)
require(MASS)
require(stringr)
require(cluster)
require(reshape)

csn.fctr <- mdl.df$cuisine
csn.rankings <- sort(table(csn.fctr), decreasing=TRUE)
csn.grp.labs <- names(csn.rankings[1:5]) # take the top 5 -- this is arbitrary though

"%w/o%" <- function(x, y) x[!x %in% y]
csn.grp.labs.excl <- levels(csn.fctr) %w/o% csn.grp.labs

csn.fctr <- factor(as.character(csn.fctr), exclude=csn.grp.labs.excl)
csn.fctr <- addNA(csn.fctr)

levels(csn.fctr) <- c(levels(csn.fctr)[1:5], 'others')
csn.fctr <- relevel(csn.fctr, ref='american-new')

reduced.df <- mdl.df
reduced.df$cuisine <- csn.fctr
reduced.df <- data.frame(reduced.df)

# A range for normalization; [just for reference]
food.range <- max(reduced.df$food) - min(reduced.df$food)


food.area.csn.glm <- glm(formula=food ~ area + cuisine, family=gaussian, data=reduced.df)
glm.food.predicts <- predict.glm(food.area.csn.glm, type="response", se.fit=TRUE)

# The Root Mean Squared Error of the Predicts
glm.food.RMSE <- sqrt(mean(sapply(FUN=function(x) x*x, glm.food.predicts$se.fit)))

# Plot the Predicts vs. actual
plot(x=reduced.df$food, y=glm.food.predicts$fit, pch=c(21, 17), col=c('dark blue', 'red'), xlab="Actual Food Ratings", ylab="Predicts", sub=paste("RMSE: ", glm.food.RMSE))


# PLot regression fit and residuals (4 panels)
dev.set(which=1) #open a new device for plotting
par(mfrow = c(2, 2), oma = c(0, 0, 2, 0))
plot(food.area.csn.glm, panel=panel.smooth)

# The principal components [PC1 = c1*area + c2*cuisine, PC2 = c3*area + c4*cuisine]
# {c1, c2, c3, c4} are coefficients that can be retrieved through rotatation matrices
dev.set(which=1) #open a new device for plotting
prcomp(x=predict.glm(food.area.csn.glm, type="terms"), data=reduced.df)
biplot(prcomp(x=predict.glm(food.area.csn.glm, type="terms"), data=reduced.df, scale=TRUE))

# For comparison, now using an "M"-estimator [robust linear model, which excludes NAs]
food.area.csn.rlm <- rlm(formula=food ~ area + cuisine, data=reduced.df, x.ret=TRUE, y.ret=TRUE, wt.method="inv.var")
rlm.food.predicts <- predict.lm(food.area.csn.rlm, type="response", se.fit=TRUE)

# The Root Mean Squared Error of the Predicts
rlm.food.RMSE <- sqrt(mean(sapply(FUN=function(x) x*x, rlm.food.predicts$se.fit)))

# Plot the Predicts vs. actual
dev.set(which=1) #open a new device for plotting
plot(x=reduced.df$food, y=rlm.food.predicts$fitted.values, pch=c(21, 17), col=c('dark blue', 'red'), xlab="Actual Food Ratings", ylab="Predicts", sub=paste("RMSE: ", rlm.food.RMSE))

# Plot regression fit and residuals (4 panels)
dev.set(which=1) #open a new device for plotting
par(mfrow = c(2, 2), oma = c(0, 0, 2, 0))
plot(food.area.csn.rlm, panel=panel.smooth)

# The principal components
dev.set(which=1) #open a new device for plotting
prcomp(x=predict.lm(food.area.csn.rlm, type="terms"), data=reduced.df)
biplot(prcomp(x=predict.lm(food.area.csn.rlm, type="terms"), data=reduced.df, scale=TRUE))

# Note Issue: graphics.off() to close all the plots


