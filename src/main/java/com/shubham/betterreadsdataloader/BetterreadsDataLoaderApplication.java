package com.shubham.betterreadsdataloader;

import com.shubham.betterreadsdataloader.author.Author;
import com.shubham.betterreadsdataloader.author.AuthorRepository;
import com.shubham.betterreadsdataloader.book.Book;
import com.shubham.betterreadsdataloader.book.BookRepository;
import connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    private void initAuthors(){
        Path path = Paths.get(authorDumpLocation);
        try(Stream<String> lines = Files.lines(path)) {

            lines.forEach(line -> {
                //Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));

                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    //construct the author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    //persist using repository
                    System.out.println("Saving author " + author.getName() + "...");
                    authorRepository.save(author);
                } catch (JSONException ex){
                    ex.printStackTrace();
                }
            });


        } catch (IOException ex){
            ex.printStackTrace();
        }
    }

    private void initWorks(){
        Path path = Paths.get(worksDumpLocation);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

        try(Stream<String> lines = Files.lines(path)) {

            lines.forEach(line -> {
                //read and parse the line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    //construct book objects
                    Book book = new Book();

                    book.setId(jsonObject.getString("key").replace("/works/", ""));

                    book.setName(jsonObject.optString("title"));

                    JSONObject jsonObjectDes = jsonObject.optJSONObject("description");
                    if(jsonObjectDes != null){
                        book.setDescription(jsonObjectDes.optString("value"));
                    }

                    JSONArray jsonArrayAuthors = jsonObject.optJSONArray("authors");
                    if(jsonArrayAuthors != null){
                        List<String> authorIds = new ArrayList<>();

                        for(int i = 0; i < jsonArrayAuthors.length(); i++){
                            String authorId = jsonArrayAuthors.getJSONObject(i).getJSONObject("author").getString("key")
                                    .replace("/authors/", "");

                            authorIds.add(authorId);
                        }

                        book.setAuthorIds(authorIds);

                        List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                    if (optionalAuthor.isEmpty()) return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());

                        book.setAuthorNames(authorNames);
                    }

                    JSONArray jsonArrayCovers = jsonObject.optJSONArray("covers");
                    if(jsonArrayCovers != null){
                        List<String> coversIds = new ArrayList<>();

                        for(int i = 0; i < jsonArrayCovers.length(); i++){
                            coversIds.add(jsonArrayCovers.getString(i));
                        }

                        book.setCoverIds(coversIds);
                    }

                    JSONObject jsonObjectPD = jsonObject.optJSONObject("created");
                    if(jsonObjectPD != null){
                        book.setPublishedDate(LocalDate.parse(jsonObjectPD.optString("value"), dateTimeFormatter));
                    }

                    bookRepository.save(book);
                    System.out.println("Saving book " + book.getName() + "...");

                } catch (JSONException ex){
                    ex.printStackTrace();
                }
            });

        } catch (IOException ex){
            ex.printStackTrace();
        }
    }

    @PostConstruct
    public void start(){
        initAuthors();
        initWorks();
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
