package com.ligx.util;

import org.fusesource.jansi.Ansi;

/**
 * Created by ligx on 16/9/22.
 */
public class ColorUtil {

  public static String red(String content){
    return Ansi.ansi().eraseScreen().fg(Ansi.Color.RED).a(content).toString();
  }

  public static String green(String content){
    return Ansi.ansi().eraseScreen().fg(Ansi.Color.GREEN).a(content).toString();
  }

  public static String blue(String content){
    return Ansi.ansi().eraseScreen().fg(Ansi.Color.BLUE).a(content).toString();
  }

  public static String white(String content){
    return Ansi.ansi().eraseScreen().fg(Ansi.Color.WHITE).a(content).toString();
  }

  public static String yellow(String content){
    return Ansi.ansi().eraseScreen().fg(Ansi.Color.YELLOW).a(content).toString();
  }

  public static void main(String[] args) {
    System.out.println(red("hello") + green("world"));
  }
}
