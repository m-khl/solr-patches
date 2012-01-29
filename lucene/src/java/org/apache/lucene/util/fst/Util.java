package org.apache.lucene.util.fst;

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

import java.io.*;
import java.util.*;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

/** Static helper methods
 *
 * @lucene.experimental */
public final class Util {
  private Util() {
  }

  /** Looks up the output for this input, or null if the
   *  input is not accepted. */
  public static<T> T get(FST<T> fst, IntsRef input) throws IOException {

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    final FST.BytesReader fstReader = fst.getBytesReader(0);

    // Accumulate output as we go
    T output = fst.outputs.getNoOutput();
    for(int i=0;i<input.length;i++) {
      if (fst.findTargetArc(input.ints[input.offset + i], arc, arc, fstReader) == null) {
        return null;
      }
      output = fst.outputs.add(output, arc.output);
    }

    if (arc.isFinal()) {
      return fst.outputs.add(output, arc.nextFinalOutput);
    } else {
      return null;
    }
  }

  // TODO: maybe a CharsRef version for BYTE2

  /** Looks up the output for this input, or null if the
   *  input is not accepted */
  public static<T> T get(FST<T> fst, BytesRef input) throws IOException {
    assert fst.inputType == FST.INPUT_TYPE.BYTE1;

    final FST.BytesReader fstReader = fst.getBytesReader(0);

    // TODO: would be nice not to alloc this on every lookup
    final FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<T>());

    // Accumulate output as we go
    T output = fst.outputs.getNoOutput();
    for(int i=0;i<input.length;i++) {
      if (fst.findTargetArc(input.bytes[i+input.offset] & 0xFF, arc, arc, fstReader) == null) {
        return null;
      }
      output = fst.outputs.add(output, arc.output);
    }

    if (arc.isFinal()) {
      return fst.outputs.add(output, arc.nextFinalOutput);
    } else {
      return null;
    }
  }

  // TODO: parameterize the FST type <T> and allow passing in a
  // comparator; eg maybe your output is a PairOutput and
  // one of the outputs in the pair is monotonic so you
  // compare by that

  /** Reverse lookup (lookup by output instead of by input),
   *  in the special case when your FSTs outputs are
   *  strictly ascending.  This locates the input/output
   *  pair where the output is equal to the target, and will
   *  return null if that output does not exist.
   *
   *  <p>NOTE: this only works with FST<Long>, only
   *  works when the outputs are ascending in order with
   *  the inputs and only works when you shared
   *  the outputs (pass doShare=true to {@link
   *  PositiveIntOutputs#getSingleton}).
   *  For example, simple ordinals (0, 1,
   *  2, ...), or file offets (when appending to a file)
   *  fit this. */
  public static IntsRef getByOutput(FST<Long> fst, long targetOutput) throws IOException {

    final FST.BytesReader in = fst.getBytesReader(0);

    // TODO: would be nice not to alloc this on every lookup
    FST.Arc<Long> arc = fst.getFirstArc(new FST.Arc<Long>());
    
    FST.Arc<Long> scratchArc = new FST.Arc<Long>();

    final IntsRef result = new IntsRef();

    long output = arc.output;
    int upto = 0;

    //System.out.println("reverseLookup output=" + targetOutput);

    while(true) {
      if (arc.isFinal()) {
        final long finalOutput = output + arc.nextFinalOutput;
        //System.out.println("  isFinal finalOutput=" + finalOutput);
        if (finalOutput == targetOutput) {
          result.length = upto;
          //System.out.println("    found!");
          return result;
        } else if (finalOutput > targetOutput) {
          //System.out.println("    not found!");
          return null;
        }
      }

      if (fst.targetHasArcs(arc)) {
        //System.out.println("  targetHasArcs");
        if (result.ints.length == upto) {
          result.grow(1+upto);
        }
        
        fst.readFirstRealTargetArc(arc.target, arc, in);

        FST.Arc<Long> prevArc = null;

        // TODO: we could do binary search here if node arcs
        // are array'd:
        while(true) {
          //System.out.println("    cycle label=" + arc.label + " output=" + arc.output);

          // This is the min output we'd hit if we follow
          // this arc:
          final long minArcOutput = output + arc.output;

          if (minArcOutput == targetOutput) {
            // Recurse on this arc:
            //System.out.println("  match!  break");
            output = minArcOutput;
            result.ints[upto++] = arc.label;
            break;
          } else if (minArcOutput > targetOutput) {
            if (prevArc == null) {
              // Output doesn't exist
              return null;
            } else {
              // Recurse on previous arc:
              arc.copyFrom(prevArc);
              result.ints[upto++] = arc.label;
              output += arc.output;
              //System.out.println("    recurse prev label=" + (char) arc.label + " output=" + output);
              break;
            }
          } else if (arc.isLast()) {
            // Recurse on this arc:
            output = minArcOutput;
            //System.out.println("    recurse last label=" + (char) arc.label + " output=" + output);
            result.ints[upto++] = arc.label;
            break;
          } else {
            // Read next arc in this node:
            prevArc = scratchArc;
            prevArc.copyFrom(arc);
            //System.out.println("      after copy label=" + (char) prevArc.label + " vs " + (char) arc.label);
            fst.readNextRealArc(arc, in);
          }
        }
      } else {
        //System.out.println("  no target arcs; not found!");
        return null;
      }
    }    
  }
  
  /**
   * Dumps an {@link FST} to a GraphViz's <code>dot</code> language description
   * for visualization. Example of use:
   * 
   * <pre>
   * PrintStream ps = new PrintStream(&quot;out.dot&quot;);
   * fst.toDot(ps);
   * ps.close();
   * </pre>
   * 
   * and then, from command line:
   * 
   * <pre>
   * dot -Tpng -o out.png out.dot
   * </pre>
   * 
   * <p>
   * Note: larger FSTs (a few thousand nodes) won't even render, don't bother.
   * 
   * @param sameRank
   *          If <code>true</code>, the resulting <code>dot</code> file will try
   *          to order states in layers of breadth-first traversal. This may
   *          mess up arcs, but makes the output FST's structure a bit clearer.
   * 
   * @param labelStates
   *          If <code>true</code> states will have labels equal to their offsets in their
   *          binary format. Expands the graph considerably. 
   * 
   * @see "http://www.graphviz.org/"
   */
  public static <T> void toDot(FST<T> fst, Writer out, boolean sameRank, boolean labelStates) 
    throws IOException {    
    final String expandedNodeColor = "blue";

    // This is the start arc in the automaton (from the epsilon state to the first state 
    // with outgoing transitions.
    final FST.Arc<T> startArc = fst.getFirstArc(new FST.Arc<T>());

    // A queue of transitions to consider for the next level.
    final List<FST.Arc<T>> thisLevelQueue = new ArrayList<FST.Arc<T>>();

    // A queue of transitions to consider when processing the next level.
    final List<FST.Arc<T>> nextLevelQueue = new ArrayList<FST.Arc<T>>();
    nextLevelQueue.add(startArc);
    //System.out.println("toDot: startArc: " + startArc);
    
    // A list of states on the same level (for ranking).
    final List<Integer> sameLevelStates = new ArrayList<Integer>();

    // A bitset of already seen states (target offset).
    final BitSet seen = new BitSet();
    seen.set(startArc.target);

    // Shape for states.
    final String stateShape = "circle";
    final String finalStateShape = "doublecircle";

    // Emit DOT prologue.
    out.write("digraph FST {\n");
    out.write("  rankdir = LR; splines=true; concentrate=true; ordering=out; ranksep=2.5; \n");

    if (!labelStates) {
      out.write("  node [shape=circle, width=.2, height=.2, style=filled]\n");      
    }

    emitDotState(out, "initial", "point", "white", "");

    final T NO_OUTPUT = fst.outputs.getNoOutput();

    // final FST.Arc<T> scratchArc = new FST.Arc<T>();

    {
      final String stateColor;
      if (fst.isExpandedTarget(startArc)) {
        stateColor = expandedNodeColor;
      } else {
        stateColor = null;
      }

      final boolean isFinal;
      final T finalOutput;
      if (startArc.isFinal()) {
        isFinal = true;
        finalOutput = startArc.nextFinalOutput == NO_OUTPUT ? null : startArc.nextFinalOutput;
      } else {
        isFinal = false;
        finalOutput = null;
      }
      
      emitDotState(out, Integer.toString(startArc.target), isFinal ? finalStateShape : stateShape, stateColor, finalOutput == null ? "" : fst.outputs.outputToString(finalOutput));
    }

    out.write("  initial -> " + startArc.target + "\n");

    int level = 0;

    final FST.BytesReader r = fst.getBytesReader(0);

    while (!nextLevelQueue.isEmpty()) {
      // we could double buffer here, but it doesn't matter probably.
      //System.out.println("next level=" + level);
      thisLevelQueue.addAll(nextLevelQueue);
      nextLevelQueue.clear();

      level++;
      out.write("\n  // Transitions and states at level: " + level + "\n");
      while (!thisLevelQueue.isEmpty()) {
        final FST.Arc<T> arc = thisLevelQueue.remove(thisLevelQueue.size() - 1);
        //System.out.println("  pop: " + arc);
        if (fst.targetHasArcs(arc)) {
          // scan all target arcs
          //System.out.println("  readFirstTarget...");
          final int node = arc.target;

          fst.readFirstRealTargetArc(arc.target, arc, r);

          //System.out.println("    firstTarget: " + arc);

          while (true) {

            //System.out.println("  cycle arc=" + arc);
            // Emit the unseen state and add it to the queue for the next level.
            if (arc.target >= 0 && !seen.get(arc.target)) {

              /*
              boolean isFinal = false;
              T finalOutput = null;
              fst.readFirstTargetArc(arc, scratchArc);
              if (scratchArc.isFinal() && fst.targetHasArcs(scratchArc)) {
                // target is final
                isFinal = true;
                finalOutput = scratchArc.output == NO_OUTPUT ? null : scratchArc.output;
                System.out.println("dot hit final label=" + (char) scratchArc.label);
              }
              */
              final String stateColor;
              if (fst.isExpandedTarget(arc)) {
                stateColor = expandedNodeColor;
              } else {
                stateColor = null;
              }

              final String finalOutput;
              if (arc.nextFinalOutput != null && arc.nextFinalOutput != NO_OUTPUT) {
                finalOutput = fst.outputs.outputToString(arc.nextFinalOutput);
              } else {
                finalOutput = "";
              }

              emitDotState(out, Integer.toString(arc.target), stateShape, stateColor, finalOutput);
              // To see the node address, use this instead:
              //emitDotState(out, Integer.toString(arc.target), stateShape, stateColor, String.valueOf(arc.target));
              seen.set(arc.target);
              nextLevelQueue.add(new FST.Arc<T>().copyFrom(arc));
              sameLevelStates.add(arc.target);
            }

            String outs;
            if (arc.output != NO_OUTPUT) {
              outs = "/" + fst.outputs.outputToString(arc.output);
            } else {
              outs = "";
            }

            if (!fst.targetHasArcs(arc) && arc.isFinal() && arc.nextFinalOutput != NO_OUTPUT) {
              // Tricky special case: sometimes, due to
              // pruning, the builder can [sillily] produce
              // an FST with an arc into the final end state
              // (-1) but also with a next final output; in
              // this case we pull that output up onto this
              // arc
              outs = outs + "/[" + fst.outputs.outputToString(arc.nextFinalOutput) + "]";
            }

            final String arcColor;
            if (arc.flag(FST.BIT_TARGET_NEXT)) {
              arcColor = "red";
            } else {
              arcColor = "black";
            }

            assert arc.label != FST.END_LABEL;
            out.write("  " + node + " -> " + arc.target + " [label=\"" + printableLabel(arc.label) + outs + "\"" + (arc.isFinal() ? " style=\"bold\"" : "" ) + " color=\"" + arcColor + "\"]\n");
                   
            // Break the loop if we're on the last arc of this state.
            if (arc.isLast()) {
              //System.out.println("    break");
              break;
            }
            fst.readNextRealArc(arc, r);
          }
        }
      }

      // Emit state ranking information.
      if (sameRank && sameLevelStates.size() > 1) {
        out.write("  {rank=same; ");
        for (int state : sameLevelStates) {
          out.write(state + "; ");
        }
        out.write(" }\n");
      }
      sameLevelStates.clear();                
    }

    // Emit terminating state (always there anyway).
    out.write("  -1 [style=filled, color=black, shape=doublecircle, label=\"\"]\n\n");
    out.write("  {rank=sink; -1 }\n");
    
    out.write("}\n");
    out.flush();
  }

  /**
   * Emit a single state in the <code>dot</code> language. 
   */
  private static void emitDotState(Writer out, String name, String shape,
      String color, String label) throws IOException {
    out.write("  " + name 
        + " [" 
        + (shape != null ? "shape=" + shape : "") + " "
        + (color != null ? "color=" + color : "") + " "
        + (label != null ? "label=\"" + label + "\"" : "label=\"\"") + " "
        + "]\n");
  }

  /**
   * Ensures an arc's label is indeed printable (dot uses US-ASCII). 
   */
  private static String printableLabel(int label) {
    if (label >= 0x20 && label <= 0x7d) {
      return Character.toString((char) label);
    } else {
      return "0x" + Integer.toHexString(label);
    }
  }

  /** Decodes the Unicode codepoints from the provided
   *  CharSequence and places them in the provided scratch
   *  IntsRef, which must not be null, returning it. */
  public static IntsRef toUTF32(CharSequence s, IntsRef scratch) {
    int charIdx = 0;
    int intIdx = 0;
    final int charLimit = s.length();
    while(charIdx < charLimit) {
      scratch.grow(intIdx+1);
      final int utf32 = Character.codePointAt(s, charIdx);
      scratch.ints[intIdx] = utf32;
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    scratch.length = intIdx;
    return scratch;
  }

  /** Decodes the Unicode codepoints from the provided
   *  char[] and places them in the provided scratch
   *  IntsRef, which must not be null, returning it. */
  public static IntsRef toUTF32(char[] s, int offset, int length, IntsRef scratch) {
    int charIdx = offset;
    int intIdx = 0;
    final int charLimit = offset + length;
    while(charIdx < charLimit) {
      scratch.grow(intIdx+1);
      final int utf32 = Character.codePointAt(s, charIdx);
      scratch.ints[intIdx] = utf32;
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    scratch.length = intIdx;
    return scratch;
  }

  /** Just takes unsigned byte values from the BytesRef and
   *  converts into an IntsRef. */
  public static IntsRef toIntsRef(BytesRef input, IntsRef scratch) {
    scratch.grow(input.length);
    for(int i=0;i<input.length;i++) {
      scratch.ints[i] = input.bytes[i+input.offset] & 0xFF;
    }
    scratch.length = input.length;
    return scratch;
  }

  /** Just converts IntsRef to BytesRef; you must ensure the
   *  int values fit into a byte. */
  public static BytesRef toBytesRef(IntsRef input, BytesRef scratch) {
    scratch.grow(input.length);
    for(int i=0;i<input.length;i++) {
      scratch.bytes[i] = (byte) input.ints[i+input.offset];
    }
    scratch.length = input.length;
    return scratch;
  }
}
