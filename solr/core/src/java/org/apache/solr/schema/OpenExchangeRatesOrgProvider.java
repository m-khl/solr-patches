package org.apache.solr.schema;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.noggit.JSONParser;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exchange Rates Provider for {@link CurrencyField} implementing the freely available
 * exchange rates from openexchangerates.org
 * <p/>
 * <b>Disclaimer:</b> This data is collected from various providers and provided free of charge
 * for informational purposes only, with no guarantee whatsoever of accuracy, validity,
 * availability or fitness for any purpose; use at your own risk. Other than that - have
 * fun, and please share/watch/fork if you think data like this should be free!
 */
public class OpenExchangeRatesOrgProvider implements ExchangeRateProvider {
  public static Logger log = LoggerFactory.getLogger(OpenExchangeRatesOrgProvider.class);
  protected static final String PARAM_RATES_FILE_LOCATION   = "ratesFileLocation";
  protected static final String PARAM_REFRESH_INTERVAL      = "refreshInterval";
  protected static final String DEFAULT_RATES_FILE_LOCATION = "http://openexchangerates.org/latest.json";
  protected static final String DEFAULT_REFRESH_INTERVAL    = "1440";
  
  protected String ratesFileLocation;
  protected int refreshInterval;
  protected ResourceLoader resourceLoader;
  
  protected OpenExchangeRates rates;

  /**
   * Returns the currently known exchange rate between two currencies. The rates are fetched from
   * the freely available OpenExchangeRates.org JSON, hourly updated. All rates are symmetrical with
   * base currency being USD by default.
   *
   * @param sourceCurrencyCode The source currency being converted from.
   * @param targetCurrencyCode The target currency being converted to.
   * @return The exchange rate.
   * @throws an exception if the requested currency pair cannot be found 
   */
  public double getExchangeRate(String sourceCurrencyCode, String targetCurrencyCode) {
    if (rates == null) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Rates not initialized.");
    }
      
    if (sourceCurrencyCode == null || targetCurrencyCode == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot get exchange rate; currency was null.");
    }
    
    if (rates.getTimestamp() + refreshInterval*60*1000 > System.currentTimeMillis()) {
      log.debug("Refresh interval has expired. Refreshing exchange rates.");
      reload();
    }
    
    Double source = (Double) rates.getRates().get(sourceCurrencyCode);
    Double target = (Double) rates.getRates().get(targetCurrencyCode);

    if (source == null || target == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
          "No available conversion rate from " + sourceCurrencyCode + " to " + targetCurrencyCode + ". "
          + "Available rates are "+listAvailableCurrencies());
    }
    
    return target / source;  
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OpenExchangeRatesOrgProvider that = (OpenExchangeRatesOrgProvider) o;

    return !(rates != null ? !rates.equals(that.rates) : that.rates != null);
  }

  @Override
  public int hashCode() {
    return rates != null ? rates.hashCode() : 0;
  }

  public String toString() {
    return "["+this.getClass().getName()+" : " + rates.getRates().size() + " rates.]";
  }

  @Override
  public Set<String> listAvailableCurrencies() {
    if (rates == null)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Rates not initialized");
    return rates.getRates().keySet();
  }

  @Override
  public boolean reload() throws SolrException {
    InputStream ratesJsonStream = null;
    try {
      log.info("Reloading exchange rates from "+ratesFileLocation);
      try {
        ratesJsonStream = (new URL(ratesFileLocation)).openStream();
      } catch (Exception e) {
        ratesJsonStream = resourceLoader.openResource(ratesFileLocation);
      }
        
      rates = new OpenExchangeRates(ratesJsonStream);
      return true;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reloading exchange rates", e);
    } finally {
      if (ratesJsonStream != null) try {
        ratesJsonStream.close();
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error closing stream", e);
      }
    }
  }

  @Override
  public void init(Map<String,String> params) throws SolrException {
    try {
      ratesFileLocation = getParam(params.get(PARAM_RATES_FILE_LOCATION), DEFAULT_RATES_FILE_LOCATION);
      refreshInterval = Integer.parseInt(getParam(params.get(PARAM_REFRESH_INTERVAL), DEFAULT_REFRESH_INTERVAL));
      // Force a refresh interval of minimum one hour, since the API does not offer better resolution
      if (refreshInterval < 60) {
        refreshInterval = 60;
        log.warn("Specified refreshInterval was too small. Setting to 60 minutes which is the update rate of openexchangerates.org");
      }
      log.info("Initialized with rates="+ratesFileLocation+", refreshInterval="+refreshInterval+".");
    } catch (Exception e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Error initializing", e);
    } finally {
      // Removing config params custom to us
      params.remove(PARAM_RATES_FILE_LOCATION);
      params.remove(PARAM_REFRESH_INTERVAL);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws SolrException {
    resourceLoader = loader;
    reload();
  }
  
  private String getParam(String param, String defaultParam) {
    return param == null ? defaultParam : param;
  }
  
  /**
   * A simple class encapsulating the JSON data from openexchangerates.org
   */
  class OpenExchangeRates {
    private Map<String, Double> rates;
    private String baseCurrency;
    private long timestamp;
    private String disclaimer;
    private String license;
    private JSONParser parser;
    
    public OpenExchangeRates(InputStream ratesStream) throws IOException {
      parser = new JSONParser(new InputStreamReader(ratesStream));
      rates = new HashMap<String, Double>();
      
      int ev;
      do {
        ev = parser.nextEvent();
        switch( ev ) {
          case JSONParser.STRING:
            if( parser.wasKey() ) {
              String key = parser.getString();
              if(key.equals("disclaimer")) {
                parser.nextEvent();
                disclaimer = parser.getString();
              } else if(key.equals("license")) {
                parser.nextEvent();
                license = parser.getString();
              } else if(key.equals("timestamp")) {
                parser.nextEvent();
                timestamp = parser.getLong();
              } else if(key.equals("base")) {
                parser.nextEvent();
                baseCurrency = parser.getString();
              } else if(key.equals("rates")) {
                ev = parser.nextEvent();
                assert(ev == JSONParser.OBJECT_START);
                ev = parser.nextEvent();
                while (ev != JSONParser.OBJECT_END) {
                  String curr = parser.getString();
                  ev = parser.nextEvent();
                  Double rate = parser.getDouble();
                  rates.put(curr, rate);
                  ev = parser.nextEvent();                  
                }
              } else {
                log.warn("Unknown key "+key);
              }
              break;
            } else {
              log.warn("Expected key, got "+JSONParser.getEventString(ev));
              break;
            }
             
          case JSONParser.OBJECT_END:
          case JSONParser.OBJECT_START:
          case JSONParser.EOF:
            break;

          default:
            log.info("Noggit UNKNOWN_EVENT_ID:"+JSONParser.getEventString(ev));
            break;
        }
      } while( ev != JSONParser.EOF);
    }

    public Map<String, Double> getRates() {
      return rates;
    }
    
    public long getTimestamp() {
      return timestamp;
    }

    public String getDisclaimer() {
      return disclaimer;
    }

    public String getBaseCurrency() {
      return baseCurrency;
    }

    public String getLicense() {
      return license;
    }
  }
}
