/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.bdgenomics.formats.avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Sequence extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Sequence\",\"namespace\":\"org.bdgenomics.formats.avro\",\"fields\":[{\"name\":\"bases\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"qualities\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence bases;
  @Deprecated public java.lang.CharSequence qualities;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Sequence() {}

  /**
   * All-args constructor.
   */
  public Sequence(java.lang.CharSequence bases, java.lang.CharSequence qualities) {
    this.bases = bases;
    this.qualities = qualities;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return bases;
    case 1: return qualities;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: bases = (java.lang.CharSequence)value$; break;
    case 1: qualities = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'bases' field.
   */
  public java.lang.CharSequence getBases() {
    return bases;
  }

  /**
   * Sets the value of the 'bases' field.
   * @param value the value to set.
   */
  public void setBases(java.lang.CharSequence value) {
    this.bases = value;
  }

  /**
   * Gets the value of the 'qualities' field.
   */
  public java.lang.CharSequence getQualities() {
    return qualities;
  }

  /**
   * Sets the value of the 'qualities' field.
   * @param value the value to set.
   */
  public void setQualities(java.lang.CharSequence value) {
    this.qualities = value;
  }

  /** Creates a new Sequence RecordBuilder */
  public static org.bdgenomics.formats.avro.Sequence.Builder newBuilder() {
    return new org.bdgenomics.formats.avro.Sequence.Builder();
  }
  
  /** Creates a new Sequence RecordBuilder by copying an existing Builder */
  public static org.bdgenomics.formats.avro.Sequence.Builder newBuilder(org.bdgenomics.formats.avro.Sequence.Builder other) {
    return new org.bdgenomics.formats.avro.Sequence.Builder(other);
  }
  
  /** Creates a new Sequence RecordBuilder by copying an existing Sequence instance */
  public static org.bdgenomics.formats.avro.Sequence.Builder newBuilder(org.bdgenomics.formats.avro.Sequence other) {
    return new org.bdgenomics.formats.avro.Sequence.Builder(other);
  }
  
  /**
   * RecordBuilder for Sequence instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Sequence>
    implements org.apache.avro.data.RecordBuilder<Sequence> {

    private java.lang.CharSequence bases;
    private java.lang.CharSequence qualities;

    /** Creates a new Builder */
    private Builder() {
      super(org.bdgenomics.formats.avro.Sequence.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(org.bdgenomics.formats.avro.Sequence.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.bases)) {
        this.bases = data().deepCopy(fields()[0].schema(), other.bases);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.qualities)) {
        this.qualities = data().deepCopy(fields()[1].schema(), other.qualities);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Sequence instance */
    private Builder(org.bdgenomics.formats.avro.Sequence other) {
            super(org.bdgenomics.formats.avro.Sequence.SCHEMA$);
      if (isValidValue(fields()[0], other.bases)) {
        this.bases = data().deepCopy(fields()[0].schema(), other.bases);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.qualities)) {
        this.qualities = data().deepCopy(fields()[1].schema(), other.qualities);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'bases' field */
    public java.lang.CharSequence getBases() {
      return bases;
    }
    
    /** Sets the value of the 'bases' field */
    public org.bdgenomics.formats.avro.Sequence.Builder setBases(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.bases = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'bases' field has been set */
    public boolean hasBases() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'bases' field */
    public org.bdgenomics.formats.avro.Sequence.Builder clearBases() {
      bases = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'qualities' field */
    public java.lang.CharSequence getQualities() {
      return qualities;
    }
    
    /** Sets the value of the 'qualities' field */
    public org.bdgenomics.formats.avro.Sequence.Builder setQualities(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.qualities = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'qualities' field has been set */
    public boolean hasQualities() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'qualities' field */
    public org.bdgenomics.formats.avro.Sequence.Builder clearQualities() {
      qualities = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public Sequence build() {
      try {
        Sequence record = new Sequence();
        record.bases = fieldSetFlags()[0] ? this.bases : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.qualities = fieldSetFlags()[1] ? this.qualities : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
