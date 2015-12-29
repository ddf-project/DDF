package io.ddf.ds;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.ddf.exception.DDFException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class UserRegistry {

  private static final UserRegistry instance = new UserRegistry();

  private static final ThreadLocal<User> currentUser = new ThreadLocal<>();

  private final LoadingCache<String, User> nameMap = CacheBuilder.newBuilder().build(
      new CacheLoader<String, User>() {
        @Override
        public User load(String username) throws Exception {
          String id = UUID.randomUUID().toString();
          User user = new User(id, username);
          idMap.put(id, user);
          return user;
        }
      });

  private final Map<String, User> idMap = new ConcurrentHashMap<>();

  private UserRegistry() {
  }

  public static UserRegistry getInstance() {
    return instance;
  }

  public static User getCurrentUser() {
    return currentUser.get();
  }

  public static void setCurrentUser(User user) {
    currentUser.set(user);
  }

  public synchronized void add(User user) {
    Preconditions.checkArgument(user != null, "user cannot be null");

    String id = user.getId();
    Preconditions.checkArgument(!idMap.containsKey(id),
        "Another user with same id is already present: " + id);

    String name = user.getUsername();
    Preconditions.checkArgument(nameMap.getIfPresent(name) == null,
        "Another user with same name is already present: " + name);

    nameMap.put(name, user);
    idMap.put(id, user);
  }

  public synchronized void remove(User user) {
    Preconditions.checkArgument(user != null, "user cannot be null");

    nameMap.invalidate(user.getUsername());
    idMap.remove(user.getId());
  }

  public User get(String id) {
    return idMap.get(id);
  }

  public User getByName(String username) {
    try {
      return nameMap.get(username);
    } catch (ExecutionException e) {
      e.printStackTrace();
      throw new RuntimeException("Error loading user", e);
    }
  }

}
