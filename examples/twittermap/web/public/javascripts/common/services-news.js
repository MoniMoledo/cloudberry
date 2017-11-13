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
      querySliceMills: parseInt(config.querySliceMills),
      getPopulationTarget: function(parameters){
        switch (parameters.geoLevel) {
          case "state":
            return {
              joinKey: ["state"],
              dataset: "twitter.dsStatePopulation",
              lookupKey: ["stateID"],
              select: ["population"],
              as: ["population"]
            };
          case "city":
            return {
              joinKey: ["city"],
              dataset: "twitter.dsCityPopulation",
              lookupKey: ["cityID"],
              select: ["population"],
              as: ["population"]
            };
        }
      }
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
          key: "totalCount"
        }
      }
    });    

    function requestLiveCounts() {
      if(ws.readyState === ws.OPEN){
        ws.send(countNewsRequest);
      }
    }
    
    var myVar = setInterval(requestLiveCounts, 1000);

    function getLevel(level){
      switch(level){
        case "state" : return "stateID";
        case "city" : return "cityID";
      }
    }

    function getFilter(parameters, maxDay, geoIds) {
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
          field: "published",
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

    function byGeoRequest(parameters, geoIds) {
        return {
          dataset: parameters.dataset,
          filter: getFilter(parameters, defaultNonSamplingDayRange, geoIds),
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
            }],
            lookup: [
              cloudberryConfig.getPopulationTarget(parameters)
            ]
          }
        };
    }

    function byTimeRequest(parameters) {
      return {
        dataset: parameters.dataset,
        filter: getFilter(parameters, defaultNonSamplingDayRange, parameters.geoIds),
        group: {
          by: [{
            field: "published",
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

      totalCount: 0,
      startDate: startDate,
      parameters: {
        dataset: "webhose.ds_news",
        keywords: [],
        timeInterval: {
          start: startDate,
          end: endDate ?  endDate : new Date()
        },
        timeBin : "day",
        geoLevel: "state",
        geoIds : [12,11,43,27,28,23,25,24,32,50,14,26,29,22,33,53,51,15,13,31,42,41,35,17,52,16,21]
      },

      mapResult: [],
      partialMapResult: [],
      timeResult: [],
      hashTagResult: [],
      errorMessage: null,

      query: function(parameters) {

        var sampleJson = (JSON.stringify({
          dataset: parameters.dataset,
          filter: getFilter(parameters, defaultSamplingDayRange, parameters.geoIds),
          select: {
            order: ["-published"],
            limit: defaultSamplingSize,
            offset: 0,
            field: ["published", "uuid"]
          },
          transform: {
            wrap: {
              key: "sample"
            }
          }
        }));

        // Batch request without map result - used when the complete map result cache hit case
        var batchWithoutGeoRequest = cloudberryConfig.querySliceMills > 0 ? (JSON.stringify({
          batch: [byTimeRequest(parameters)],
          option: {
            sliceMillis: cloudberryConfig.querySliceMills
          },
          transform: {
            wrap: {
              key: "batchWithoutGeoRequest"
            }
          }
        })) : (JSON.stringify({
            batch: [byTimeRequest(parameters)],
            transform: {
                wrap: {
                    key: "batchWithoutGeoRequest"
                }
            }
        }));

        // Gets the Geo IDs that are not in the map result cache.
        geoIdsNotInCache = MapResultCache.getGeoIdsNotInCache(cloudberryService.parameters.keywords,
          cloudberryService.parameters.timeInterval,
          cloudberryService.parameters.geoIds, cloudberryService.parameters.geoLevel);

        // Batch request with only the geoIds whose map result are not cached yet - partial map result cache hit case
        // This case also covers the complete cache miss case.
        var batchWithPartialGeoRequest = cloudberryConfig.querySliceMills > 0 ? (JSON.stringify({
          batch: [byTimeRequest(parameters), byGeoRequest(parameters, geoIdsNotInCache)],
          option: {
            sliceMillis: cloudberryConfig.querySliceMills
          },
          transform: {
            wrap: {
              key: "batchWithPartialGeoRequest"
            }
          }
        })) : (JSON.stringify({
            batch: [byTimeRequest(parameters), byGeoRequest(parameters, geoIdsNotInCache)],
            transform: {
                wrap: {
                    key: "batchWithPartialGeoRequest"
                }
            }
        }));

        // Complete map result cache hit case - exclude map result request
        if(geoIdsNotInCache.length === 0)  {
          cloudberryService.mapResult = MapResultCache.getValues(cloudberryService.parameters.geoIds,
            cloudberryService.parameters.geoLevel);

          ws.send(sampleJson);
          ws.send(batchWithoutGeoRequest);
        }
        // Partial map result cache hit case
        else  {
          cloudberryService.partialMapResult = MapResultCache.getValues(cloudberryService.parameters.geoIds,
                cloudberryService.parameters.geoLevel);

          ws.send(sampleJson);
          ws.send(batchWithPartialGeoRequest);
        }
      }
    };

    ws.onmessage = function(event) {
      $timeout(function() {
        var result = JSONbig.parse(event.data);

        switch (result.key) {

          case "sample":
            cloudberryService.tweetResult = result.value[0];
            break;
          // Complete cache hit case
          case "batchWithoutGeoRequest":
            if(angular.isArray(result.value)) {
              cloudberryService.timeResult = result.value[0];
            }
            break;
          // Partial map result cache hit or complete cache miss case
          case "batchWithPartialGeoRequest":
            if(angular.isArray(result.value)) {
              cloudberryService.timeResult = result.value[0];
              cloudberryService.mapResult = result.value[1].concat(cloudberryService.partialMapResult);
            }
            // When the query is executed completely, we update the map result cache.
            if((cloudberryConfig.querySliceMills > 0 && !angular.isArray(result.value) &&
                result.value['key'] === "done") || cloudberryConfig.querySliceMills <= 0) {
              MapResultCache.putValues(geoIdsNotInCache, cloudberryService.parameters.geoLevel,
                cloudberryService.mapResult);
            }
            break;
          case "totalCount":
            cloudberryService.totalCount = result.value[0][0].count;
            break;
          case "error":
            console.error(result);
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