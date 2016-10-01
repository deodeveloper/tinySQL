/*
 * This class provides a very primitive XM tokenizer.
 *
 * $Author: $
 * $Date: $
 * $Revision: $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Revision History:
 * 
 * Written by Davis Swan in April, 2005.
 */
package com.sqlmagic.tinysql;

import java.text.*;
import java.util.*;
import java.lang.*;

public class SimpleXMLTag extends Hashtable
{
   String[] fields;
   int fieldIndex;
   Vector tagList;
   int tagIndex = 0,tagEndAt;
   String tagName,tagLabel;
   boolean debug = false;
/*
 * Parse out the body text of an XML tag.  The result is a string 
 * or another XMLTokenizer object.  Note that if this is a child
 * of an existing XML tag, a Vector of the tag hierarchy is passed in
 * so that tags can be processed in the order they were encountered.
 */
   public SimpleXMLTag(String inputString) throws Exception
   {
      this(inputString,0,new Vector());
   }
   public SimpleXMLTag(String inputString,int startAt,Vector inputList)
      throws Exception
   {
      char quoteChar,nextChar,bracketQuoteChar;
      char[] charArray = {' '};
      SimpleXMLTag embeddedXML;
      int i,j,startBodyAt,endBodyAt,tagNameEndAt,embeddedTagAt;
      String testString,bodyString;
      tagList = inputList;
      if ( inputString.length() == 0 ) 
         throw new Exception("XML record is null.");
/*
 *    Search the body for the ending tag, ignoring everything in quotes.
 */
      quoteChar = ' ';
      embeddedTagAt = -1;
      tagName = (String)null;
      tagNameEndAt = Integer.MIN_VALUE;
      startBodyAt = Integer.MIN_VALUE;
      for ( i = startAt; i < inputString.length(); i++ )
      {
         nextChar = inputString.charAt(i);
         if ( nextChar == '\'' | nextChar == '"' )
         {
            if ( quoteChar == ' ' )
               quoteChar = nextChar; 
            else if ( nextChar == quoteChar )
               quoteChar = ' ';
         }
/*
 *       Ignore all characters in quoted strings.
 */
         if ( quoteChar != ' ' ) continue;
         if ( nextChar == '<' ) 
         {
            if ( tagName == (String)null ) 
            {
/*
 *             The tag has been found.  Save any text before the tag as a label.
 */
               tagNameEndAt = inputString.indexOf('>',i);
               if ( tagNameEndAt < 0 ) 
                  throw new Exception("Tag is incomplete");
               tagName = inputString.substring(i + 1,tagNameEndAt);
               if ( debug ) System.out.println("Found tag " + tagName);
               startBodyAt = tagNameEndAt + 1;
               for ( j = tagNameEndAt + 1; j < inputString.length(); j++ )
               {
                  if ( inputString.charAt(j) == ' ') continue;
                  startBodyAt = j;
                  if ( debug ) System.out.println("Start body at " + j);
                  break;
               }
            } else {
               if ( i  + tagName.length() + 1 > inputString.length() - 1 ) 
                  throw new Exception("XML record ends prematurely.");
/*
 *             Search for the end of the tag.
 */
               testString = inputString.substring(i+1,i+tagName.length()+2);
               if ( testString.equals("/" + tagName) )
               {
                  endBodyAt = i - 1;
                  if ( debug) System.out.println("End body at " + endBodyAt);
                  bodyString = inputString.substring(startBodyAt,endBodyAt+1);
                  tagEndAt = i + tagName.length() + 2;
                  if ( debug) System.out.println("Tag body is " + bodyString);
                  put("TAG_NAME",tagName);
                  tagList.addElement(this);
                  if ( embeddedTagAt > -1 )
                  {
/*
 *                   The tag body contains one or more embedded tags
 *                   which have to be parsed recursively.
 */
                     if ( embeddedTagAt > startBodyAt )
                     {
                        tagLabel = inputString.substring(startBodyAt,embeddedTagAt).trim();
                        if ( tagLabel.length() > 0 ) 
                        {
                           if ( debug ) System.out.println("label " + tagLabel);
                           put("LABEL",tagLabel);
                        }
                     }
                     while ( embeddedTagAt < endBodyAt )
                     {
                        embeddedXML = new SimpleXMLTag(inputString,
                                                   embeddedTagAt,tagList);
                        put(tagName,embeddedXML);
                        if ( debug ) System.out.println("Tag " 
                        + embeddedXML.tagName + " ends at "
                        + embeddedXML.tagEndAt);
                        embeddedTagAt = embeddedXML.tagEndAt + 1;
                     }
                  } else {
                     put(tagName,bodyString);
                  }
                  break;
               } else if ( embeddedTagAt == -1) {
                  embeddedTagAt = i;
                  if ( debug )
                     System.out.println("embeddedTagAt=" + embeddedTagAt);
               }
            }
         }
      }
   }
/*
 * Return the total number of tags in the hierarchy.
 */
   public int countTags()
   {
      return tagList.size();
   }
/*
 * Return a particular tag by its index in the hierarchy.
 */
   public SimpleXMLTag getTag(int inputIndex)
   {
      if ( inputIndex < 0 | inputIndex > tagList.size() - 1 )
         return (SimpleXMLTag)null;
      return (SimpleXMLTag)tagList.elementAt(inputIndex);
   }
/*
 * Methods used to get tags sequentially.
 */
   public boolean hasMoreTags()
   {
      if ( tagIndex < tagList.size() - 1 ) return true;
      else return false;
   }
   public SimpleXMLTag nextTag()
   {
      if ( tagIndex > -1 & tagIndex < tagList.size() - 1 )
      {
         tagIndex++;
         return (SimpleXMLTag)tagList.elementAt(tagIndex);
      } else {
         return (SimpleXMLTag)null;
      }
   }
   public String toString()
   {
      Object XMLElement;
      StringBuffer outputBuffer;
      String tagName,objectType,tagLabel;
      SimpleXMLTag sxml;
      tagName = (String)get("TAG_NAME"); 
      outputBuffer = new StringBuffer("<" + tagName + ">");
      tagLabel = (String)get("LABEL"); 
      if ( tagLabel != (String)null )
         outputBuffer.append(tagLabel);
      XMLElement = get(tagName);
      objectType = XMLElement.getClass().getName();
      if ( debug ) System.out.println("Tag object type is " + objectType);
      if ( objectType.endsWith("java.lang.String") )
      {
         outputBuffer.append((String)XMLElement);
      } else {
         sxml = (SimpleXMLTag)XMLElement;
      }
      outputBuffer.append("\n</" + tagName + ">");
      return outputBuffer.toString();
   }
}
