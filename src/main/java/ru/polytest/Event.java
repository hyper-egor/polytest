package ru.polytest;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Event {
    private String id;
    private String ticker;
    private String slug;
    private String title;
    private String description;
    private String resolutionSource;
    private String startDate;
    private String creationDate;
    private String endDate;
    private String image;
    private String icon;
    private Boolean active;
    private Boolean closed;
    private Boolean archived;
    private Boolean featured;
    private Boolean restricted;
    private Double liquidity;
    private Double volume;
    private Double openInterest;
    private String sortBy;
    private String category;
    @SerializedName("published_at")
    private String publishedAt;
    private String createdAt;
    private String updatedAt;
    private Double competitive;
    private Double volume24hr;
    private Double volume1wk;
    private Double volume1mo;
    private Double volume1yr;
    private Double liquidityAmm;
    private Boolean enableOrderBook;
    private Double liquidityClob;
    private Boolean negRisk;
    private Integer commentCount;
    private List<Market> markets;
    private Boolean cyom;
    private String closedTime;
    private Boolean showAllOutcomes;
    private Boolean showMarketImages;
    private Boolean enableNegRisk;
    private Boolean automaticallyActive;
    private String gmpChartMode;
    private Boolean negRiskAugmented;
    private Boolean cumulativeMarkets;
    private String seriesSlug;
    private Boolean pendingDeployment;
    private Boolean deploying;
    private Boolean requiresTranslation;
    @SerializedName("new")
    private Boolean isNew;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public String getSlug() {
        return slug;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getResolutionSource() {
        return resolutionSource;
    }

    public void setResolutionSource(String resolutionSource) {
        this.resolutionSource = resolutionSource;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
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

    public Boolean getArchived() {
        return archived;
    }

    public void setArchived(Boolean archived) {
        this.archived = archived;
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

    public Double getLiquidity() {
        return liquidity;
    }

    public void setLiquidity(Double liquidity) {
        this.liquidity = liquidity;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }

    public Double getOpenInterest() {
        return openInterest;
    }

    public void setOpenInterest(Double openInterest) {
        this.openInterest = openInterest;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(String publishedAt) {
        this.publishedAt = publishedAt;
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

    public Double getCompetitive() {
        return competitive;
    }

    public void setCompetitive(Double competitive) {
        this.competitive = competitive;
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

    public Double getLiquidityAmm() {
        return liquidityAmm;
    }

    public void setLiquidityAmm(Double liquidityAmm) {
        this.liquidityAmm = liquidityAmm;
    }

    public Boolean getEnableOrderBook() {
        return enableOrderBook;
    }

    public void setEnableOrderBook(Boolean enableOrderBook) {
        this.enableOrderBook = enableOrderBook;
    }

    public Double getLiquidityClob() {
        return liquidityClob;
    }

    public void setLiquidityClob(Double liquidityClob) {
        this.liquidityClob = liquidityClob;
    }

    public Boolean getNegRisk() {
        return negRisk;
    }

    public void setNegRisk(Boolean negRisk) {
        this.negRisk = negRisk;
    }

    public Integer getCommentCount() {
        return commentCount;
    }

    public void setCommentCount(Integer commentCount) {
        this.commentCount = commentCount;
    }

    public List<Market> getMarkets() {
        return markets;
    }

    public void setMarkets(List<Market> markets) {
        this.markets = markets;
    }

    public Boolean getCyom() {
        return cyom;
    }

    public void setCyom(Boolean cyom) {
        this.cyom = cyom;
    }

    public String getClosedTime() {
        return closedTime;
    }

    public void setClosedTime(String closedTime) {
        this.closedTime = closedTime;
    }

    public Boolean getShowAllOutcomes() {
        return showAllOutcomes;
    }

    public void setShowAllOutcomes(Boolean showAllOutcomes) {
        this.showAllOutcomes = showAllOutcomes;
    }

    public Boolean getShowMarketImages() {
        return showMarketImages;
    }

    public void setShowMarketImages(Boolean showMarketImages) {
        this.showMarketImages = showMarketImages;
    }

    public Boolean getEnableNegRisk() {
        return enableNegRisk;
    }

    public void setEnableNegRisk(Boolean enableNegRisk) {
        this.enableNegRisk = enableNegRisk;
    }

    public Boolean getAutomaticallyActive() {
        return automaticallyActive;
    }

    public void setAutomaticallyActive(Boolean automaticallyActive) {
        this.automaticallyActive = automaticallyActive;
    }

    public String getGmpChartMode() {
        return gmpChartMode;
    }

    public void setGmpChartMode(String gmpChartMode) {
        this.gmpChartMode = gmpChartMode;
    }

    public Boolean getNegRiskAugmented() {
        return negRiskAugmented;
    }

    public void setNegRiskAugmented(Boolean negRiskAugmented) {
        this.negRiskAugmented = negRiskAugmented;
    }

    public Boolean getCumulativeMarkets() {
        return cumulativeMarkets;
    }

    public void setCumulativeMarkets(Boolean cumulativeMarkets) {
        this.cumulativeMarkets = cumulativeMarkets;
    }

    public String getSeriesSlug() {
        return seriesSlug;
    }

    public void setSeriesSlug(String seriesSlug) {
        this.seriesSlug = seriesSlug;
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

    public Boolean getRequiresTranslation() {
        return requiresTranslation;
    }

    public void setRequiresTranslation(Boolean requiresTranslation) {
        this.requiresTranslation = requiresTranslation;
    }

    public Boolean getNew() {
        return isNew;
    }

    public void setNew(Boolean aNew) {
        isNew = aNew;
    }
}
