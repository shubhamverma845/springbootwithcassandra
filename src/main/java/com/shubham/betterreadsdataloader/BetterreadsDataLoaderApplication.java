package com.shubham.betterreadsdataloader;

import com.shubham.betterreadsdataloader.author.Author;
import com.shubham.betterreadsdataloader.author.AuthorRepository;
import connection.DataStaxAstraProperties;
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
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    private void initAuthors(){
        Path path = Paths.get(authorDumpLocation);
//        System.out.println(path.getFileName());
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
