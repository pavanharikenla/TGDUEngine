package com.tg.tgduengine.util;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.AfterTest;

public class RandomGenTest {
	public RandomGen rd = null;
  @BeforeTest
  public void beforeTest() {
	  rd = new RandomGen();
  }

  @AfterTest
  public void afterTest() {
	  rd = null;
  }


  @Test
  public void email() {
    rd.getEmail();
  }

  @Test
  public void mobileNum() {
    rd.getMobileNum();
  }
}
