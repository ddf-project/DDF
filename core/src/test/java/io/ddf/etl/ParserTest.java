package io.ddf.etl;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

/**
 * Created by huandao on 10/6/15.
 */
public class ParserTest {

  @Test
  public void test() throws JSQLParserException {
    String command = "SELECT dob, weight, height, sex, countryname, regionname, cityname, firstsynctime, lastsynctime, "
        + "avgdailysyncs, firstplatform, avgdailysteps, avgdailypoints, activedays, avgdailypoints_activedays, "
        + "avgsleepminutes, avgdeepsleepminutes, numsleepsessions, test_1 FROM {1} WHERE "
        + "((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((("
        + "(((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((countryname = '') "
        + "OR (countryname = 'Canada')) OR (countryname = 'Fiji')) OR (countryname = 'Turkmenistan')) OR (countryname = "
        + "'Lithuania')) OR (countryname = 'Cambodia')) OR (countryname = 'Ethiopia')) OR (countryname = 'Aruba')) OR"
        + " (countryname = 'Palestine')) OR (countryname = 'Argentina')) OR (countryname = 'Bolivia')) OR (countryname = "
        + "'Cameroon')) OR (countryname = 'Bahrain')) OR (countryname = 'Saudi Arabia')) OR (countryname = 'Saint Barthlemy'))"
        + " OR (countryname = 'Cape Verde')) OR (countryname = 'Slovenia')) OR (countryname = 'Guatemala')) OR "
        + "(countryname = 'Bosnia and Herzegovina')) OR (countryname = 'Germany')) OR (countryname = 'Spain')) OR "
        + "(countryname = 'Maldives')) OR (countryname = 'Paraguay')) OR (countryname = 'Jamaica')) OR (countryname = "
        + "'Oman')) OR (countryname = 'Tanzania')) OR (countryname = 'Greenland')) OR (countryname = 'French Guiana')) "
        + "OR (countryname = 'Monaco')) OR (countryname = 'New Zealand')) OR (countryname = 'Yemen')) OR (countryname ="
        + " 'Jersey')) OR (countryname = 'Pakistan')) OR (countryname = 'United Arab Emirates')) OR (countryname = 'Guam'))"
        + " OR (countryname = 'India')) OR (countryname = 'Azerbaijan')) OR (countryname = 'Madagascar')) OR (countryname = "
        + "'Saint Vincent and the Grenadines')) OR (countryname = 'Cyprus')) OR (countryname = 'South Korea')) OR "
        + "(countryname = 'Belarus')) OR (countryname = 'Tajikistan')) OR (countryname = 'Turkey')) OR (countryname = "
        + "'Northern Mariana Islands')) OR (countryname = 'San Marino')) OR (countryname = 'Kyrgyzstan')) OR (countryname = "
        + "'Mongolia')) OR (countryname = 'France')) OR (countryname = 'Bermuda')) OR (countryname = 'Slovakia')) OR "
        + "(countryname = 'Peru')) OR (countryname = 'Laos')) OR (countryname = 'Norway')) OR (countryname = 'Malawi'))"
        + " OR (countryname = 'Federated States of Micronesia')) OR (countryname = 'Cuba')) OR (countryname = "
        + "'Montenegro')) OR (countryname = 'Saint Kitts and Nevis')) OR (countryname = 'Togo')) OR (countryname = "
        + "'China')) OR (countryname = 'Armenia')) OR (countryname = 'Republic of Korea')) OR (countryname = 'Dominican Republic'))"
        + " OR (countryname = 'Ukraine')) OR (countryname = 'Ghana')) OR (countryname = 'Finland')) OR (countryname = 'Libya')) "
        + "OR (countryname = 'Indonesia')) OR (countryname = 'Mauritius')) OR (countryname = 'Liechtenstein')) OR (countryname = 'Vietnam')) "
        + "OR (countryname = 'British Virgin Islands')) OR (countryname = 'Mali')) OR (countryname = 'Russia')) OR"
        + " (countryname = 'Bulgaria')) OR (countryname = 'United States')) OR (countryname = 'Romania')) OR "
        + "(countryname = 'Angola')) OR (countryname = 'Cayman Islands')) OR (countryname = 'South Africa')) OR "
        + "(countryname = 'Macao')) OR (countryname = 'Sweden')) OR (countryname = 'Malaysia')) OR"
        + " (countryname = 'Austria')) OR (countryname = 'Mozambique')) OR (countryname = 'Uganda')) OR "
        + "(countryname = 'Japan')) OR (countryname = 'Isle of Man')) OR (countryname = 'Brazil')) OR"
        + " (countryname = 'U.S. Virgin Islands')) OR (countryname = 'Kuwait')) OR (countryname = 'Panama')) "
        + "OR (countryname = 'Guyana')) OR (countryname = 'Republic of Moldova')) OR (countryname = 'Costa Rica')) "
        + "OR (countryname = 'Luxembourg')) OR (countryname = 'Bahamas')) OR (countryname = 'Gibraltar'))"
        + " OR (countryname = 'Ivory Coast')) OR (countryname = 'Syria')) OR (countryname = 'Nigeria')) OR "
        + "(countryname = 'Ecuador')) OR (countryname = 'Bangladesh')) OR (countryname = 'Brunei')) OR"
        + " (countryname = 'Australia')) OR (countryname = 'Iran')) OR (countryname = 'Algeria')) OR "
        + "(countryname = 'Republic of Lithuania')) OR (countryname = 'El Salvador')) OR "
        + "(countryname = 'Czech Republic')) OR (countryname = 'Runion')) OR (countryname = 'Chile')) OR (countryname = "
        + "'Puerto Rico')) OR (countryname = 'Belgium')) OR (countryname = 'Thailand')) OR (countryname = 'Haiti'))"
        + " OR (countryname = 'Belize')) OR (countryname = 'Hong Kong')) OR (countryname = 'Georgia')) OR "
        + "(countryname = 'Denmark')) OR (countryname = 'Philippines')) OR (countryname = 'Moldova')) OR "
        + "(countryname = 'Morocco')) OR (countryname = 'Croatia')) OR (countryname = 'French Polynesia')) OR "
        + "(countryname = 'Guernsey')) OR (countryname = 'Switzerland')) OR (countryname = 'Grenada')) OR "
        + "(countryname = 'Myanmar [Burma]')) OR (countryname = 'Seychelles')) OR (countryname = 'Portugal')) OR "
        + "(countryname = 'Estonia')) OR (countryname = 'Uruguay')) OR (countryname = 'Mexico')) OR "
        + "(countryname = 'Lebanon')) OR (countryname = 'Uzbekistan')) OR (countryname = 'Egypt')) OR "
        + "(countryname = 'Djibouti')) OR (countryname = 'Rwanda')) OR (countryname = 'Antigua and Barbuda')) "
        + "OR (countryname = 'Colombia')) OR (countryname = 'Taiwan')) OR (countryname = 'Turks and Caicos Islands')) "
        + "OR (countryname = 'Barbados')) OR (countryname = 'Curaao')) OR (countryname = 'Qatar')) OR "
        + "(countryname = 'Italy')) OR (countryname = 'Kenya')) OR (countryname = 'Sudan')) OR (countryname = 'Nepal'))"
        + " OR (countryname = 'Malta')) OR (countryname = 'Netherlands')) OR (countryname = 'Suriname')) OR "
        + "(countryname = 'Venezuela')) OR (countryname = 'Israel')) OR (countryname = 'Réunion')) OR "
        + "(countryname = 'Iceland')) OR (countryname = 'Zambia')) OR (countryname = 'Senegal')) OR "
        + "(countryname = 'Papua New Guinea')) OR (countryname = 'Gabon')) OR (countryname = 'Zimbabwe')) OR"
        + " (countryname = 'Jordan')) OR (countryname = 'Martinique')) OR (countryname = 'Saint Martin')) OR "
        + "(countryname = 'Kazakhstan')) OR (countryname = 'Poland')) OR (countryname = 'Ireland')) OR "
        + "(countryname = 'Iraq')) OR (countryname = 'New Caledonia')) OR (countryname = 'Andorra')) OR "
        + "(countryname = 'Trinidad and Tobago')) OR (countryname = 'Latvia')) OR (countryname = 'Hungary')) OR "
        + "(countryname = 'Hashemite Kingdom of Jordan')) OR (countryname = 'Guadeloupe')) OR (countryname = 'Honduras'))"
        + " OR (countryname = 'Equatorial Guinea')) OR (countryname = 'Tunisia')) OR (countryname = 'Nicaragua')) OR"
        + " (countryname = 'Singapore')) OR (countryname = 'Serbia')) OR (countryname = 'United Kingdom')) OR "
        + "(countryname = 'Congo')) OR (countryname = 'Greece')) OR (countryname = 'Sri Lanka')) OR (countryname = 'Curaçao'))";
    CCJSqlParserManager parserManager = new CCJSqlParserManager();
    System.out.println(">>> aaaaa");
    StringReader reader = new StringReader(command);
    System.out.println(">>> bbbbb");
    parserManager.parse(reader);
    System.out.println(">>> cccccc");
  }
}
