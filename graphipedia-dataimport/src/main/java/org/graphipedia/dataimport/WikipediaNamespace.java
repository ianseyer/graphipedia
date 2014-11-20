package org.graphipedia.dataimport;

import java.util.HashMap;
import java.util.Map;

public class WikipediaNamespace {
	
	private static final Map<String, String> category;
    static
    {
    	category = new HashMap<String, String>();
    	category.put("ar", "تصنيف");
    	category.put("de", "Kategorie");
    	category.put("el", "Κατηγορία");
    	category.put("en", "Category");
    	category.put("es", "Categoría");
    	category.put("fr", "Catégorie");
    	category.put("hu", "Kategória");
    	category.put("it", "Categoria");
    	category.put("ja", "Category");
    	category.put("nl", "Categorie");
    	category.put("pl", "Kategoria");
    	category.put("pt", "Categoria");
    	category.put("ru", "Категория");
    	category.put("sv", "Kategori");
    	category.put("vi", "Thể loại");
    	category.put("war", "Kaarangay");
    	category.put("zh", "Category");
    }
    
    public static String getCategoryName(String lang) {
    	return category.get(lang);
    }

}
