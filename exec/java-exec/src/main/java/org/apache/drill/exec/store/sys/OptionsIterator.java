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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.options.DrillConfigIterator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.options.SystemOptionManager;

public class OptionsIterator implements Iterator<Object> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionIterator.class);

  enum Mode {
    BOOT, SYS_SESS, BOTH ,NEW_OPTIONS
  };

  private final OptionManager fragmentOptions;
  private final Iterator<OptionValue> mergedOptions;

  public OptionsIterator(FragmentContext context, Mode mode){
    final DrillConfigIterator configOptions = new DrillConfigIterator(context.getConfig());
    fragmentOptions = context.getOptions();
    final Iterator<OptionValue> optionList;
    optionList = sortOptions(fragmentOptions.iterator());
    List<OptionValue> values = Lists.newArrayList(optionList);
    System.out.println(values);
    Collections.sort(values);
    System.out.println(values);
    System.out.println(values.get(0).getValue());
    mergedOptions = values.iterator();

  }


  public Iterator<OptionValue> sortOptions(Iterator<OptionValue> options)
  {
    System.out.println(options);
    List<OptionValue> values = Lists.newArrayList(options);
    Collections.sort(values);
    for (int i = 1; i < values.size(); i++ )
    {
      OptionValue current = values.get(i);
      OptionValue previous = values.get(i-1);

      if(current.name.equals(previous.name)) {
        if(current.type == OptionType.SESSION) {
          values.remove(i - 1);
      }
      else if(current.type == OptionType.SYSTEM && previous.type == OptionType.DEFAULT) {
          values.remove(i - 1);
      }
      else {
          values.remove(i);
      }
      }
    }
    return values.iterator();
  }

  @Override
  public boolean hasNext() {
    return mergedOptions.hasNext();
  }

  @Override
  public ResultObjectWrapper next() {
    final OptionValue value = mergedOptions.next();
    final Status status;
    if (value.type == OptionType.BOOT) {
      status = Status.BOOT;
    } else {
      final OptionValue def = SystemOptionManager.getValidator(value.name).getResultDefault();
      if (value.equalsIgnoreType(def)) {
        status = Status.DEFAULT;
        } else {
        status = Status.CHANGED;
        }
      }
    return new ResultObjectWrapper(value.name, value.kind, value.type,value.getValue().toString(), status);
  }

  public static enum Status {
    BOOT, DEFAULT, CHANGED
  }

  /**
   * Wrapper class for OptionValue to add Status
   */
  public static class ResultObjectWrapper {

    public final String name;
    public final Kind kind;
    public final OptionType type;
    public final Status status;
    public final String string_value;


    public ResultObjectWrapper(final String name, final Kind kind, final OptionType type, final String value,
        final Status status) {
      this.name = name;
      this.kind = kind;
      this.type = type;
      this.string_value = value;
      this.status = status;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}


