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

package org.apache.solr.common;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;

/**
 *
 */
public class SolrException extends RuntimeException {

  /**
   * @since solr 1.2
   */
  public enum ErrorCode {
    BAD_REQUEST( 400 ),
    UNAUTHORIZED( 401 ),
    FORBIDDEN( 403 ),
    NOT_FOUND( 404 ),
    SERVER_ERROR( 500 ),
    SERVICE_UNAVAILABLE( 503 ),
    UNKNOWN(0);
    public final int code;
    
    private ErrorCode( int c )
    {
      code = c;
    }
    public static ErrorCode getErrorCode(int c){
      for (ErrorCode err : values()) {
        if(err.code == c) return err;
      }
      return UNKNOWN;
    }
  };

  public SolrException(ErrorCode code, String msg) {
    super(msg);
    this.code = code.code;
  }
  public SolrException(ErrorCode code, String msg, Throwable th) {
    super(msg, th);
    this.code = code.code;
  }

  public SolrException(ErrorCode code, Throwable th) {
    super(th);
    this.code = code.code;
  }
  
  int code=0;
  public int code() { return code; }


  public void log(Logger log) { log(log,this); }
  public static void log(Logger log, Throwable e) {
    if (e instanceof SolrException
        && ((SolrException) e).code() == ErrorCode.SERVICE_UNAVAILABLE.code) {
      return;
    }
    String stackTrace = toStr(e);
    String ignore = doIgnore(e, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);

  }

  public static void log(Logger log, String msg, Throwable e) {
    if (e instanceof SolrException
        && ((SolrException) e).code() == ErrorCode.SERVICE_UNAVAILABLE.code) {
      log(log, msg);
    }
    String stackTrace = msg + ':' + toStr(e);
    String ignore = doIgnore(e, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);
  }
  
  public static void log(Logger log, String msg) {
    String stackTrace = msg;
    String ignore = doIgnore(null, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);
  }

  // public String toString() { return toStr(this); }  // oops, inf loop
  @Override
  public String toString() { return super.toString(); }

  public static String toStr(Throwable e) {   
    CharArrayWriter cw = new CharArrayWriter();
    PrintWriter pw = new PrintWriter(cw);
    e.printStackTrace(pw);
    pw.flush();
    return cw.toString();

/** This doesn't work for some reason!!!!!
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    pw.flush();
    System.out.println("The STRING:" + sw.toString());
    return sw.toString();
**/
  }


  /** For test code - do not log exceptions that match any of the regular expressions in ignorePatterns */
  public static Set<String> ignorePatterns;

  /** Returns null if this exception does not match any ignore patterns, or a message string to use if it does. */
  public static String doIgnore(Throwable t, String m) {
    if (ignorePatterns == null || m == null) return null;
    if (t != null && t instanceof AssertionError) return null;

    for (String regex : ignorePatterns) {
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(m);
      
      if (matcher.find()) return "Ignoring exception matching " + regex;
    }

    return null;
  }
  
  public static Throwable getRootCause(Throwable t) {
    while (true) {
      Throwable cause = t.getCause();
      if (cause!=null) {
        t = cause;
      } else {
        break;
      }
    }
    return t;
  }

}
