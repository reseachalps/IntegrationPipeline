//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.8-b130911.1802 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2018.01.08 at 07:46:09 PM CET 
//


package org.openarchives.oai._2;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for granularityType.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="granularityType">
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *     &lt;enumeration value="YYYY-MM-DD"/>
 *     &lt;enumeration value="YYYY-MM-DDThh:mm:ssZ"/>
 *   &lt;/restriction>
 * &lt;/simpleType>
 * </pre>
 * 
 */
@XmlType(name = "granularityType")
@XmlEnum
public enum GranularityType {

    @XmlEnumValue("YYYY-MM-DD")
    YYYY_MM_DD("YYYY-MM-DD"),
    @XmlEnumValue("YYYY-MM-DDThh:mm:ssZ")
    YYYY_MM_DD_THH_MM_SS_Z("YYYY-MM-DDThh:mm:ssZ");
    private final String value;

    GranularityType(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static GranularityType fromValue(String v) {
        for (GranularityType c: GranularityType.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
