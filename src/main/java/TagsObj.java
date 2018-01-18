import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

public class TagsObj {

	private String tagName;
	
	static FlatMapFunction<TagsObj,TagsObj> mapToTags = tag -> {
		String[] tagsArray = tag.getTagName().split("\\|");
		List<TagsObj> tagsList = new ArrayList<>();
		for(String eachTag : tagsArray) {
			tagsList.add(new TagsObj(eachTag));
		}
		return tagsList.iterator();
	};	
	
	public TagsObj() {
		this.tagName=null;
	}
	public TagsObj(String tagName) {
		this.tagName=tagName;
	}

	public String getTagName() {
		return tagName;
	}

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

}
