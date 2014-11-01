/**
 *
 */
package io.ddf.misc;


import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class ALoggable {
  protected final Logger mLog = LoggerFactory.getLogger(this.getClass());

  static {
    // Get log4j going even in the absence of log4j configuration file
    // See http://logging.apache.org/log4j/1.2/faq.html#noconfig
    BasicConfigurator.configure();
  }
}
