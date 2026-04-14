package ru.polytest;

import com.google.gson.annotations.SerializedName;
import com.google.gson.annotations.JsonAdapter;

import java.util.List;

public class Market {
    private String id;
    private String question;
    private String conditionId;
    private String slug;
    private String resolutionSource;
    private String endDate;
    private String liquidity;
    private String startDate;
    private String image;
    private String icon;
    private String description;
    @JsonAdapter(StringListJsonAdapter.class)
    private List<String> outcomes;
    @JsonAdapter(StringListJsonAdapter.class)
    private List<String> outcomePrices;
    private String volume;
    private Boolean active;
    private Boolean closed;
    private String marketMakerAddress;
    private String createdAt;
    private String updatedAt;
    @SerializedName("submitted_by")
    private String submittedBy;
    private Boolean archived;
    private String resolvedBy;
    private Boolean featured;
    private Boolean restricted;
    private String groupItemTitle;
    private String groupItemThreshold;
    private String questionID;
    private Boolean enableOrderBook;
    private Double orderPriceMinTickSize;
    private Double orderMinSize;
    private Double volumeNum;
    private Double liquidityNum;
    private String endDateIso;
    private String startDateIso;
    private Boolean hasReviewedDates;
    private Double volume24hr;
    private Double volume1wk;
    private Double volume1mo;
    private Double volume1yr;
    @JsonAdapter(StringListJsonAdapter.class)
    private List<String> clobTokenIds;
    private String umaBond;
    private String umaReward;
    private Double volume24hrClob;
    private Double volume1wkClob;
    private Double volume1moClob;
    private Double volume1yrClob;
    private Double volumeClob;
    private Double liquidityClob;
    private Integer customLiveness;
    private Boolean acceptingOrders;
    private Boolean negRisk;
    private String negRiskRequestID;
    private Boolean ready;
    private Boolean funded;
    private String acceptingOrdersTimestamp;
    private Boolean cyom;
    private Double competitive;
    private Boolean pagerDutyNotificationEnabled;
    private Boolean approved;
    private Integer rewardsMinSize;
    private Double rewardsMaxSpread;
    private Double spread;
    private Double oneDayPriceChange;
    private Double oneWeekPriceChange;
    private Double oneMonthPriceChange;
    private Double lastTradePrice;
    private Double bestBid;
    private Double bestAsk;
    private Boolean automaticallyActive;
    private Boolean clearBookOnStart;
    private String seriesColor;
    private Boolean showGmpSeries;
    private Boolean showGmpOutcome;
    private Boolean manualActivation;
    private Boolean negRiskOther;
    @JsonAdapter(StringListJsonAdapter.class)
    private List<String> umaResolutionStatuses;
    private Boolean pendingDeployment;
    private Boolean deploying;
    private String deployingTimestamp;
    private Boolean rfqEnabled;
    private Boolean holdingRewardsEnabled;
    private Boolean feesEnabled;
    private Boolean requiresTranslation;
    private String feeType;
    @SerializedName("new")
    private Boolean isNew;
    private List<Event> events;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getConditionId() {
        return conditionId;
    }

    public void setConditionId(String conditionId) {
        this.conditionId = conditionId;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public String getResolutionSource() {
        return resolutionSource;
    }

    public void setResolutionSource(String resolutionSource) {
        this.resolutionSource = resolutionSource;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getLiquidity() {
        return liquidity;
    }

    public void setLiquidity(String liquidity) {
        this.liquidity = liquidity;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getOutcomes() {
        return outcomes;
    }

    public void setOutcomes(List<String> outcomes) {
        this.outcomes = outcomes;
    }

    public List<String> getOutcomePrices() {
        return outcomePrices;
    }

    public void setOutcomePrices(List<String> outcomePrices) {
        this.outcomePrices = outcomePrices;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean getClosed() {
        return closed;
    }

    public void setClosed(Boolean closed) {
        this.closed = closed;
    }

    public String getMarketMakerAddress() {
        return marketMakerAddress;
    }

    public void setMarketMakerAddress(String marketMakerAddress) {
        this.marketMakerAddress = marketMakerAddress;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getSubmittedBy() {
        return submittedBy;
    }

    public void setSubmittedBy(String submittedBy) {
        this.submittedBy = submittedBy;
    }

    public Boolean getArchived() {
        return archived;
    }

    public void setArchived(Boolean archived) {
        this.archived = archived;
    }

    public String getResolvedBy() {
        return resolvedBy;
    }

    public void setResolvedBy(String resolvedBy) {
        this.resolvedBy = resolvedBy;
    }

    public Boolean getFeatured() {
        return featured;
    }

    public void setFeatured(Boolean featured) {
        this.featured = featured;
    }

    public Boolean getRestricted() {
        return restricted;
    }

    public void setRestricted(Boolean restricted) {
        this.restricted = restricted;
    }

    public String getGroupItemTitle() {
        return groupItemTitle;
    }

    public void setGroupItemTitle(String groupItemTitle) {
        this.groupItemTitle = groupItemTitle;
    }

    public String getGroupItemThreshold() {
        return groupItemThreshold;
    }

    public void setGroupItemThreshold(String groupItemThreshold) {
        this.groupItemThreshold = groupItemThreshold;
    }

    public String getQuestionID() {
        return questionID;
    }

    public void setQuestionID(String questionID) {
        this.questionID = questionID;
    }

    public Boolean getEnableOrderBook() {
        return enableOrderBook;
    }

    public void setEnableOrderBook(Boolean enableOrderBook) {
        this.enableOrderBook = enableOrderBook;
    }

    public Double getOrderPriceMinTickSize() {
        return orderPriceMinTickSize;
    }

    public void setOrderPriceMinTickSize(Double orderPriceMinTickSize) {
        this.orderPriceMinTickSize = orderPriceMinTickSize;
    }

    public Double getOrderMinSize() {
        return orderMinSize;
    }

    public void setOrderMinSize(Double orderMinSize) {
        this.orderMinSize = orderMinSize;
    }

    public Double getVolumeNum() {
        return volumeNum;
    }

    public void setVolumeNum(Double volumeNum) {
        this.volumeNum = volumeNum;
    }

    public Double getLiquidityNum() {
        return liquidityNum;
    }

    public void setLiquidityNum(Double liquidityNum) {
        this.liquidityNum = liquidityNum;
    }

    public String getEndDateIso() {
        return endDateIso;
    }

    public void setEndDateIso(String endDateIso) {
        this.endDateIso = endDateIso;
    }

    public String getStartDateIso() {
        return startDateIso;
    }

    public void setStartDateIso(String startDateIso) {
        this.startDateIso = startDateIso;
    }

    public Boolean getHasReviewedDates() {
        return hasReviewedDates;
    }

    public void setHasReviewedDates(Boolean hasReviewedDates) {
        this.hasReviewedDates = hasReviewedDates;
    }

    public Double getVolume24hr() {
        return volume24hr;
    }

    public void setVolume24hr(Double volume24hr) {
        this.volume24hr = volume24hr;
    }

    public Double getVolume1wk() {
        return volume1wk;
    }

    public void setVolume1wk(Double volume1wk) {
        this.volume1wk = volume1wk;
    }

    public Double getVolume1mo() {
        return volume1mo;
    }

    public void setVolume1mo(Double volume1mo) {
        this.volume1mo = volume1mo;
    }

    public Double getVolume1yr() {
        return volume1yr;
    }

    public void setVolume1yr(Double volume1yr) {
        this.volume1yr = volume1yr;
    }

    public List<String> getClobTokenIds() {
        return clobTokenIds;
    }

    public void setClobTokenIds(List<String> clobTokenIds) {
        this.clobTokenIds = clobTokenIds;
    }

    public String getUmaBond() {
        return umaBond;
    }

    public void setUmaBond(String umaBond) {
        this.umaBond = umaBond;
    }

    public String getUmaReward() {
        return umaReward;
    }

    public void setUmaReward(String umaReward) {
        this.umaReward = umaReward;
    }

    public Double getVolume24hrClob() {
        return volume24hrClob;
    }

    public void setVolume24hrClob(Double volume24hrClob) {
        this.volume24hrClob = volume24hrClob;
    }

    public Double getVolume1wkClob() {
        return volume1wkClob;
    }

    public void setVolume1wkClob(Double volume1wkClob) {
        this.volume1wkClob = volume1wkClob;
    }

    public Double getVolume1moClob() {
        return volume1moClob;
    }

    public void setVolume1moClob(Double volume1moClob) {
        this.volume1moClob = volume1moClob;
    }

    public Double getVolume1yrClob() {
        return volume1yrClob;
    }

    public void setVolume1yrClob(Double volume1yrClob) {
        this.volume1yrClob = volume1yrClob;
    }

    public Double getVolumeClob() {
        return volumeClob;
    }

    public void setVolumeClob(Double volumeClob) {
        this.volumeClob = volumeClob;
    }

    public Double getLiquidityClob() {
        return liquidityClob;
    }

    public void setLiquidityClob(Double liquidityClob) {
        this.liquidityClob = liquidityClob;
    }

    public Integer getCustomLiveness() {
        return customLiveness;
    }

    public void setCustomLiveness(Integer customLiveness) {
        this.customLiveness = customLiveness;
    }

    public Boolean getAcceptingOrders() {
        return acceptingOrders;
    }

    public void setAcceptingOrders(Boolean acceptingOrders) {
        this.acceptingOrders = acceptingOrders;
    }

    public Boolean getNegRisk() {
        return negRisk;
    }

    public void setNegRisk(Boolean negRisk) {
        this.negRisk = negRisk;
    }

    public String getNegRiskRequestID() {
        return negRiskRequestID;
    }

    public void setNegRiskRequestID(String negRiskRequestID) {
        this.negRiskRequestID = negRiskRequestID;
    }

    public Boolean getReady() {
        return ready;
    }

    public void setReady(Boolean ready) {
        this.ready = ready;
    }

    public Boolean getFunded() {
        return funded;
    }

    public void setFunded(Boolean funded) {
        this.funded = funded;
    }

    public String getAcceptingOrdersTimestamp() {
        return acceptingOrdersTimestamp;
    }

    public void setAcceptingOrdersTimestamp(String acceptingOrdersTimestamp) {
        this.acceptingOrdersTimestamp = acceptingOrdersTimestamp;
    }

    public Boolean getCyom() {
        return cyom;
    }

    public void setCyom(Boolean cyom) {
        this.cyom = cyom;
    }

    public Double getCompetitive() {
        return competitive;
    }

    public void setCompetitive(Double competitive) {
        this.competitive = competitive;
    }

    public Boolean getPagerDutyNotificationEnabled() {
        return pagerDutyNotificationEnabled;
    }

    public void setPagerDutyNotificationEnabled(Boolean pagerDutyNotificationEnabled) {
        this.pagerDutyNotificationEnabled = pagerDutyNotificationEnabled;
    }

    public Boolean getApproved() {
        return approved;
    }

    public void setApproved(Boolean approved) {
        this.approved = approved;
    }

    public Integer getRewardsMinSize() {
        return rewardsMinSize;
    }

    public void setRewardsMinSize(Integer rewardsMinSize) {
        this.rewardsMinSize = rewardsMinSize;
    }

    public Double getRewardsMaxSpread() {
        return rewardsMaxSpread;
    }

    public void setRewardsMaxSpread(Double rewardsMaxSpread) {
        this.rewardsMaxSpread = rewardsMaxSpread;
    }

    public Double getSpread() {
        return spread;
    }

    public void setSpread(Double spread) {
        this.spread = spread;
    }

    public Double getOneDayPriceChange() {
        return oneDayPriceChange;
    }

    public void setOneDayPriceChange(Double oneDayPriceChange) {
        this.oneDayPriceChange = oneDayPriceChange;
    }

    public Double getOneWeekPriceChange() {
        return oneWeekPriceChange;
    }

    public void setOneWeekPriceChange(Double oneWeekPriceChange) {
        this.oneWeekPriceChange = oneWeekPriceChange;
    }

    public Double getOneMonthPriceChange() {
        return oneMonthPriceChange;
    }

    public void setOneMonthPriceChange(Double oneMonthPriceChange) {
        this.oneMonthPriceChange = oneMonthPriceChange;
    }

    public Double getLastTradePrice() {
        return lastTradePrice;
    }

    public void setLastTradePrice(Double lastTradePrice) {
        this.lastTradePrice = lastTradePrice;
    }

    public Double getBestBid() {
        return bestBid;
    }

    public void setBestBid(Double bestBid) {
        this.bestBid = bestBid;
    }

    public Double getBestAsk() {
        return bestAsk;
    }

    public void setBestAsk(Double bestAsk) {
        this.bestAsk = bestAsk;
    }

    public Boolean getAutomaticallyActive() {
        return automaticallyActive;
    }

    public void setAutomaticallyActive(Boolean automaticallyActive) {
        this.automaticallyActive = automaticallyActive;
    }

    public Boolean getClearBookOnStart() {
        return clearBookOnStart;
    }

    public void setClearBookOnStart(Boolean clearBookOnStart) {
        this.clearBookOnStart = clearBookOnStart;
    }

    public String getSeriesColor() {
        return seriesColor;
    }

    public void setSeriesColor(String seriesColor) {
        this.seriesColor = seriesColor;
    }

    public Boolean getShowGmpSeries() {
        return showGmpSeries;
    }

    public void setShowGmpSeries(Boolean showGmpSeries) {
        this.showGmpSeries = showGmpSeries;
    }

    public Boolean getShowGmpOutcome() {
        return showGmpOutcome;
    }

    public void setShowGmpOutcome(Boolean showGmpOutcome) {
        this.showGmpOutcome = showGmpOutcome;
    }

    public Boolean getManualActivation() {
        return manualActivation;
    }

    public void setManualActivation(Boolean manualActivation) {
        this.manualActivation = manualActivation;
    }

    public Boolean getNegRiskOther() {
        return negRiskOther;
    }

    public void setNegRiskOther(Boolean negRiskOther) {
        this.negRiskOther = negRiskOther;
    }

    public List<String> getUmaResolutionStatuses() {
        return umaResolutionStatuses;
    }

    public void setUmaResolutionStatuses(List<String> umaResolutionStatuses) {
        this.umaResolutionStatuses = umaResolutionStatuses;
    }

    public Boolean getPendingDeployment() {
        return pendingDeployment;
    }

    public void setPendingDeployment(Boolean pendingDeployment) {
        this.pendingDeployment = pendingDeployment;
    }

    public Boolean getDeploying() {
        return deploying;
    }

    public void setDeploying(Boolean deploying) {
        this.deploying = deploying;
    }

    public String getDeployingTimestamp() {
        return deployingTimestamp;
    }

    public void setDeployingTimestamp(String deployingTimestamp) {
        this.deployingTimestamp = deployingTimestamp;
    }

    public Boolean getRfqEnabled() {
        return rfqEnabled;
    }

    public void setRfqEnabled(Boolean rfqEnabled) {
        this.rfqEnabled = rfqEnabled;
    }

    public Boolean getHoldingRewardsEnabled() {
        return holdingRewardsEnabled;
    }

    public void setHoldingRewardsEnabled(Boolean holdingRewardsEnabled) {
        this.holdingRewardsEnabled = holdingRewardsEnabled;
    }

    public Boolean getFeesEnabled() {
        return feesEnabled;
    }

    public void setFeesEnabled(Boolean feesEnabled) {
        this.feesEnabled = feesEnabled;
    }

    public Boolean getRequiresTranslation() {
        return requiresTranslation;
    }

    public void setRequiresTranslation(Boolean requiresTranslation) {
        this.requiresTranslation = requiresTranslation;
    }

    public String getFeeType() {
        return feeType;
    }

    public void setFeeType(String feeType) {
        this.feeType = feeType;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public Boolean getNew() {
        return isNew;
    }

    public void setNew(Boolean aNew) {
        isNew = aNew;
    }
}
