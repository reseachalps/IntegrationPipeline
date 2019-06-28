//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.8-b130911.1802 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2018.01.08 at 07:46:09 PM CET 
//


package eu.openaire.namespace.oaf;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;choice maxOccurs="unbounded">
 *         &lt;element name="legalname" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="legalshortname" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="logourl" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="originalId" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="websiteurl" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="country" type="{http://namespace.openaire.eu/oaf}optionalClassedSchemedElement"/>
 *         &lt;element name="ecenterprise" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="echighereducation" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ecinternationalorganization" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ecinternationalorganizationeurinterests" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="eclegalbody" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="eclegalperson" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ecnonprofit" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ecnutscode" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ecresearchorganization" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ecsmevalidated" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="collectedfrom" type="{http://namespace.openaire.eu/oaf}namedIdElement"/>
 *         &lt;element name="pid" type="{http://namespace.openaire.eu/oaf}optionalClassedSchemedElement"/>
 *         &lt;element name="datainfo" type="{http://namespace.openaire.eu/oaf}datainfoType"/>
 *         &lt;element name="rels" type="{http://namespace.openaire.eu/oaf}relsType"/>
 *         &lt;element name="children" type="{http://namespace.openaire.eu/oaf}childrenOrg"/>
 *       &lt;/choice>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "legalnameOrLegalshortnameOrLogourl"
})
@XmlRootElement(name = "organization")
public class Organization {

    @XmlElementRefs({
        @XmlElementRef(name = "eclegalbody", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "collectedfrom", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "originalId", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecnonprofit", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecnutscode", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "websiteurl", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "eclegalperson", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecsmevalidated", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "echighereducation", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "pid", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "datainfo", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "rels", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "legalname", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecinternationalorganizationeurinterests", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "children", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecenterprise", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecinternationalorganization", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "country", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "ecresearchorganization", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "legalshortname", type = JAXBElement.class, required = false),
        @XmlElementRef(name = "logourl", type = JAXBElement.class, required = false)
    })
    protected List<JAXBElement<?>> legalnameOrLegalshortnameOrLogourl;

    /**
     * Gets the value of the legalnameOrLegalshortnameOrLogourl property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the legalnameOrLegalshortnameOrLogourl property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getLegalnameOrLegalshortnameOrLogourl().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link NamedIdElement }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link OptionalClassedSchemedElement }{@code >}
     * {@link JAXBElement }{@code <}{@link DatainfoType }{@code >}
     * {@link JAXBElement }{@code <}{@link RelsType }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link ChildrenOrg }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link OptionalClassedSchemedElement }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * {@link JAXBElement }{@code <}{@link String }{@code >}
     * 
     * 
     */
    public List<JAXBElement<?>> getLegalnameOrLegalshortnameOrLogourl() {
        if (legalnameOrLegalshortnameOrLogourl == null) {
            legalnameOrLegalshortnameOrLogourl = new ArrayList<JAXBElement<?>>();
        }
        return this.legalnameOrLegalshortnameOrLogourl;
    }

}
