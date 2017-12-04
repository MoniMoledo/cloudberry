angular.module('cloudberry.common', ['cloudberry.mapresultcache'])
  .factory('cloudberryConfig', function(){
    return {
      ws: config.wsURL,
      sentimentEnabled: config.sentimentEnabled,
      sentimentUDF: config.sentimentUDF,
      removeSearchBar: config.removeSearchBar,
      predefinedKeywords: config.predefinedKeywords,
      normalizationUpscaleFactor: 1000 * 1000,
      normalizationUpscaleText: "/M",
      sentimentUpperBound: 4,
      cacheThreshold: parseInt(config.cacheThreshold),
      querySliceMills: parseInt(config.querySliceMills)
    };
  })
  .service('cloudberry', function($timeout, cloudberryConfig, MapResultCache) {
    var startDate = config.startDate;
    var endDate = config.endDate;
    var defaultNonSamplingDayRange = 1500;
    var defaultSamplingDayRange = 30;
    var defaultSamplingSize = 10;
    var ws = new WebSocket(cloudberryConfig.ws);
    // The MapResultCache.getGeoIdsNotInCache() method returns the geoIds
    // not in the cache for the current query.
    var geoIdsNotInCache = [];

  var countNewsRequest = JSON.stringify({
      dataset: "webhose.ds_news",
      global: {
        globalAggregate: {
          field: "*",
          apply: {
            name: "count"
          },
          as: "count"
        }},
      estimable : true,
      transform: {
        wrap: {
          key: "newsCount"
        }
      }
    });

  var countTweetRequest = JSON.stringify({
      dataset: "twitter.ds_tweet",
      global: {
        globalAggregate: {
          field: "*",
          apply: {
            name: "count"
          },
          as: "count"
        }},
      estimable : true,
      transform: {
        wrap: {
          key: "tweetCount"
        }
      }
    });       

    function requestLiveCounts() {
      if(ws.readyState === ws.OPEN){
        ws.send(countNewsRequest);
        ws.send(countTweetRequest);
      }
    }
    
    var myVar = setInterval(requestLiveCounts, 1000);

    function getLevel(level){
      switch(level){
        case "state" : return "stateID";
        case "city" : return "cityID";
      }
    }

    function getTimeField(dataset){
      switch(dataset){
        case "webhose.ds_news" : return "published";
        case "twitter.ds_tweet" : return "create_at";
      }
    }

    function getIdField(dataset){
      switch(dataset){
        case "webhose.ds_news" : return "uuid";
        case "twitter.ds_tweet" : return "id";
      }
    }

    function getFilter(parameters, maxDay, geoIds, dataset) {
      var spatialField = getLevel(parameters.geoLevel);
      var keywords = [];
      for(var i = 0; i < parameters.keywords.length; i++){
        keywords.push(parameters.keywords[i].replace("\"", "").trim());
      }
      var queryStartDate = new Date(parameters.timeInterval.end);
      queryStartDate.setDate(queryStartDate.getDate() - maxDay);
      queryStartDate = parameters.timeInterval.start > queryStartDate ? parameters.timeInterval.start : queryStartDate;

      var filter = [
        {
          field: getTimeField(dataset),
          relation: "inRange",
          values: [queryStartDate.toISOString(), parameters.timeInterval.end.toISOString()]
        }, {
          field: "text",
          relation: "contains",
          values: keywords
        }
      ];
      if (geoIds.length <= 2000){
        filter.push(
          {
            field: "geo_tag." + spatialField,
            relation: "in",
            values: geoIds
          }
        );
      }
      return filter;
    }

    function byGeoRequest(parameters, geoIds, dataset) {
        return {
          dataset: dataset,
          filter: getFilter(parameters, defaultNonSamplingDayRange, geoIds, dataset),
          group: {
            by: [{
              field: "geo",
              apply: {
                name: "level",
                args: {
                  level: parameters.geoLevel
                }
              },
              as: parameters.geoLevel
            }],
            aggregate: [{
              field: "*",
              apply: {
                name: "count"
              },
              as: "count"
            }]
          }
        };
    }

    function byTimeRequest(parameters, dataset) {
      return {
        dataset: dataset,
        filter: getFilter(parameters, defaultNonSamplingDayRange, parameters.geoIds, dataset),
        group: {
          by: [{
            field: getTimeField(dataset),
            apply: {
              name: "interval",
              args: {
                unit: parameters.timeBin
              }
            },
            as: parameters.timeBin
          }],
          aggregate: [{
            field: "*",
            apply: {
              name: "count"
            },
            as: "count"
          }]
        }
      };
    }

    var cloudberryService = {

      newsCount: 0,
      tweetCount: 0,
      startDate: startDate,
      parameters: {
        datasets: ["webhose.ds_news", "twitter.ds_tweet"],
        sources: [],
        keywords: [],
        timeInterval: {
          start: startDate,
          end: endDate ?  endDate : new Date()
        },
        timeBin : "day",
        geoLevel: "state",
        geoIds : [12,11,43,27,28,23,25,24,32,50,14,26,29,22,33,53,51,15,13,31,42,41,35,17,52,16,21]
      },
      newsMapResult: [],
      tweetMapResult: [],
      tPartialMapResult: [],
      nPartialMapResult: [],
      tweetTimeResult: [],
      newsTimeResult: [],
      errorMessage: null,

      query: function(parameters) {
        var newsDataset = "webhose.ds_news";
        var tweetDataset = "twitter.ds_tweet";
        var tweetSampleJson = (JSON.stringify({
          dataset: tweetDataset,
          filter: getFilter(parameters, defaultSamplingDayRange, parameters.geoIds, tweetDataset),
          select: {
            order: ["-"+ getTimeField(tweetDataset)],
            limit: defaultSamplingSize,
            offset: 0,
            field: [getTimeField(tweetDataset), getIdField(tweetDataset)]
          },
          transform: {
            wrap: {
              key: "tweetSample"
            }
          }
        }));


          // Batch request without map result - used when the complete map result cache hit case
         var tweetBatchWithoutGeoRequest = cloudberryConfig.querySliceMills > 0 ? (JSON.stringify({
            batch: [byTimeRequest(parameters, tweetDataset)],
            option: {
              sliceMillis: cloudberryConfig.querySliceMills
            },
            transform: {
              wrap: {
                key: "tweetBatchWithoutGeoRequest"
              }
            }
          })) : (JSON.stringify({
              batch: [byTimeRequest(parameters, tweetDataset)],
              transform: {
                  wrap: {
                      key: "tweetBatchWithoutGeoRequest"
                  }
              }
          }));

          var newsBatchWithoutGeoRequest = cloudberryConfig.querySliceMills > 0 ? (JSON.stringify({
            batch: [byTimeRequest(parameters, newsDataset)],
            option: {
              sliceMillis: cloudberryConfig.querySliceMills
            },
            transform: {
              wrap: {
                key: "newsBatchWithoutGeoRequest"
              }
            }
          })) : (JSON.stringify({
              batch: [byTimeRequest(parameters, newsDataset)],
              transform: {
                  wrap: {
                      key: "newsBatchWithoutGeoRequest"
                  }
              }
          }));

        // Gets the Geo IDs that are not in the map result cache.
        geoIdsNotInCache = MapResultCache.getGeoIdsNotInCache(cloudberryService.parameters.keywords,
          cloudberryService.parameters.timeInterval,
          cloudberryService.parameters.geoIds, cloudberryService.parameters.geoLevel, cloudberryService.parameters.sources);

        // Batch request with only the geoIds whose map result are not cached yet - partial map result cache hit case
        // This case also covers the complete cache miss case.
        var tweetBatchWithPartialGeoRequest = cloudberryConfig.querySliceMills > 0 ? (JSON.stringify({
          batch: [byTimeRequest(parameters, tweetDataset), byGeoRequest(parameters, geoIdsNotInCache, tweetDataset)],
          option: {
            sliceMillis: cloudberryConfig.querySliceMills
          },
          transform: {
            wrap: {
              key: "tweetBatchWithPartialGeoRequest"
            }
          }
        })) : (JSON.stringify({
            batch: [byTimeRequest(parameters, tweetDataset), byGeoRequest(parameters, geoIdsNotInCache, tweetDataset)],
            transform: {
                wrap: {
                    key: "tweetBatchWithPartialGeoRequest"
                }
            }
        }));

        var newsBatchWithPartialGeoRequest = cloudberryConfig.querySliceMills > 0 ? (JSON.stringify({
          batch: [byTimeRequest(parameters, newsDataset), byGeoRequest(parameters, geoIdsNotInCache, newsDataset)],
          option: {
            sliceMillis: cloudberryConfig.querySliceMills
          },
          transform: {
            wrap: {
              key: "newsBatchWithPartialGeoRequest"
            }
          }
        })) : (JSON.stringify({
            batch: [byTimeRequest(parameters,newsDataset), byGeoRequest(parameters, geoIdsNotInCache,newsDataset)],
            transform: {
                wrap: {
                    key: "newsBatchWithPartialGeoRequest"
                }
            }
        }));

        // Complete map result cache hit case - exclude map result request
        if(geoIdsNotInCache.length === 0)  {
          cloudberryService.tweetMapResult = MapResultCache.getValues(cloudberryService.parameters.geoIds,
            cloudberryService.parameters.geoLevel);
          cloudberryService.newsMapResult = MapResultCache.getValues(cloudberryService.parameters.geoIds,
            cloudberryService.parameters.geoLevel);
          if(cloudberryService.parameters.sources.includes("twitter")) {
            ws.send(tweetSampleJson);
            ws.send(tweetBatchWithoutGeoRequest);
          }
          if(cloudberryService.parameters.sources.includes("news")) {
            ws.send(newsBatchWithoutGeoRequest);
          }
          if(!cloudberryService.parameters.sources.includes("news")) {
            cloudberryService.newsMapResult = [];
          }
          if(!cloudberryService.parameters.sources.includes("twitter")) {
              cloudberryService.tweetMapResult = [];
            }
        }
        // Partial map result cache hit case
        else  {
          if(cloudberryService.parameters.sources.includes("twitter")) {
          cloudberryService.tPartialMapResult = MapResultCache.getValues(cloudberryService.parameters.geoIds,
                cloudberryService.parameters.geoLevel);
            ws.send(tweetSampleJson);
            ws.send(tweetBatchWithPartialGeoRequest);
          }
          if(cloudberryService.parameters.sources.includes("news")) {
          cloudberryService.nPartialMapResult = MapResultCache.getValues(cloudberryService.parameters.geoIds,
                cloudberryService.parameters.geoLevel);
            ws.send(newsBatchWithPartialGeoRequest);
          }
          if(!cloudberryService.parameters.sources.includes("news")) {
            cloudberryService.newsMapResult = [];
            cloudberryService.nPartialMapResult = [];
          }
          if(!cloudberryService.parameters.sources.includes("twitter")) {
            cloudberryService.tPartialMapResult = [];
            cloudberryService.tweetMapResult = [];
          }
        }
      }
    };

    ws.onmessage = function(event) {
      $timeout(function() {
        var result = JSONbig.parse(event.data);
        cloudberryService.tweetResult = [];
        switch (result.key) {
          case "tweetSample":
            cloudberryService.tweetResult = result.value[0];
            break;
          // Complete cache hit case
          case "tweetBatchWithoutGeoRequest":
            if(angular.isArray(result.value)) {
              cloudberryService.tweetTimeResult = result.value[0];
            }
            break;
             // Complete cache hit case
          case "newsBatchWithoutGeoRequest":
            if(angular.isArray(result.value)) {
              cloudberryService.newsTimeResult = result.value[0];
            }
            break;
          // Partial map result cache hit or complete cache miss case
          case "tweetBatchWithPartialGeoRequest":
            if(angular.isArray(result.value)) {
              cloudberryService.tweetTimeResult = result.value[0];
              cloudberryService.tweetMapResult = result.value[1].concat(cloudberryService.tPartialMapResult);
            }
            // When the query is executed completely, we update the map result cache.
            if((cloudberryConfig.querySliceMills > 0 && !angular.isArray(result.value) &&
                result.value['key'] === "done") || cloudberryConfig.querySliceMills <= 0) {
              MapResultCache.putValues(geoIdsNotInCache, cloudberryService.parameters.geoLevel,
                cloudberryService.tweetMapResult);
            }
            break;
           case "newsBatchWithPartialGeoRequest":
            if(angular.isArray(result.value)) {
              cloudberryService.newsTimeResult = result.value[0];
              cloudberryService.newsMapResult = result.value[1].concat(cloudberryService.nPartialMapResult);
            }
            // When the query is executed completely, we update the map result cache.
            if((cloudberryConfig.querySliceMills > 0 && !angular.isArray(result.value) &&
                result.value['key'] === "done") || cloudberryConfig.querySliceMills <= 0) {
              MapResultCache.putValues(geoIdsNotInCache, cloudberryService.parameters.geoLevel,
                cloudberryService.newsMapResult);
            }
            break;
          case "newsCount":
            cloudberryService.newsCount = result.value[0][0].count;
            break;
          case "tweetCount":
            cloudberryService.tweetCount = result.value[0][0].count;
            break;
          case "error":
            cloudberryService.errorMessage = result.value;
            break;
          case "done":
            break;
          default:
            console.error("ws get unknown data: ", result);
            cloudberryService.errorMessage = "ws get unknown data: " + result.toString();
            break;
        }
      });
    };

    return cloudberryService;
  });