/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.sys;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Comparator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.options.SystemOptionManager;
/*
 * To extend the original Option iterator to hide the implementation details and to return the row
 * which takes precedence over others for an option.This is done by examining the type as the precendence
 * order is session - system - default.All the values are represented as String instead of having multiple
 * columns and the datatype is provided for type details.
 */
public class ExtendedOptionIterator implements Iterator<Object> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionIterator.class);

  private final OptionManager fragmentOptions;
  private final Iterator<OptionValue> mergedOptions;

  public ExtendedOptionIterator(FragmentContext context) {
    fragmentOptions = context.getOptions();
    final Iterator<OptionValue> optionList;
    mergedOptions = sortOptions(fragmentOptions.iterator());
  }
  /*
    Sort options according to name and the type to remove the redundant rows
    for the same option based on the type
   */
  public Iterator<OptionValue> sortOptions(Iterator<OptionValue> options)
  {
    List<OptionValue> values = Lists.newArrayList(options);
    List<OptionValue> optionValues = Lists.newArrayList();
    OptionValue temp = null;
    OptionValue value;
    OptionType type;

    Collections.sort(values,  new Comparator<OptionValue>() {
      @Override
      public int compare(OptionValue v1, OptionValue v2) {
        int nameCmp = v1.name.compareTo(v2.name);
        if (nameCmp != 0) {
          return nameCmp;
        }
        return v1.type.compareTo( v2.type);
      }
    });

    for (int i = 0; i < values.size() ;i++ )
    {
      value = values.get(i);
      type = value.type ;
      switch (type) {
        case DEFAULT:
          temp = value;
          break;
        case SESSION:
          temp = value;
          break;
        case SYSTEM:
          if(!temp.getName().equals(value.getName())) {
            temp = value;
          }
          else if (temp.getName().equals(value.getName()) && temp.type.equals(OptionType.DEFAULT)) {
            temp = value;
          }
          break;
      }
      if(i == values.size() - 1 || (i < values.size()  && !temp.getName().equals(values.get(i+1).getName()) ) ) {
        optionValues.add(temp);
      }

    }
    return optionValues.iterator();
  }

  @Override
  public boolean hasNext() {
    return mergedOptions.hasNext();
  }

  @Override
  public ExtendedOptionValueWrapper next() {
    final OptionValue value = mergedOptions.next();
    return new ExtendedOptionValueWrapper(value.name, value.kind, value.type,value.getValue().toString());
  }

  public static enum Status {
    BOOT, DEFAULT, CHANGED
  }

  /**
   * Wrapper class for Extended Option Value
   */
  public static class ExtendedOptionValueWrapper {

    public final String name;
    public final Kind kind;
    public final OptionType type;
    public final String string_value;


    public ExtendedOptionValueWrapper(final String name, final Kind kind, final OptionType type, final String value) {
      this.name = name;
      this.kind = kind;
      this.type = type;
      this.string_value = value;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}


