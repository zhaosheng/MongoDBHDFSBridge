package com.linkedin.rosetta.mongo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Created with IntelliJ IDEA.
 * User: shezhao
 * Date: 7/8/14
 * Time: 6:07 PM
 *
 */
public class Status {
  private int _insertRecordCount;
  private Map<String, String> _messages;

  public Status() {
    _messages = new HashMap<String, String>();
  }

  public boolean addMessage(String title, String msg) {
    boolean overwriteFlag = false;
    if (_messages.containsKey(title)) {
      overwriteFlag = true;
    }
    _messages.put(title, msg);
    return overwriteFlag;
  }

  public String getMessage(String title) {
    return _messages.get(title);
  }

  public Set<String> getMessageTitles() {
    return _messages.keySet();
  }

  public void removeMessage(String title) {
    _messages.remove(title);
  }

  public int getInsertRecordCount() {
    return _insertRecordCount;
  }

  public void increaseInsertCount(int delta) {
    _insertRecordCount += delta;
  }
}
