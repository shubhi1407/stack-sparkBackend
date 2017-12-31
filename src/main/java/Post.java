import java.io.Serializable;

public class Post implements Serializable{
  private String Location;
  private Integer id;
  
  public Integer getId() {
	return id;
}
public void setId(Integer id) {
	this.id = id;
}



public String getLocation() {
	return Location;
}
public void setLocation(String location) {
	Location = location;
}

  
}
