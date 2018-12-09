import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * Created by zb on 2018/12/9.
 */
@SpringBootApplication
public class SpringBootApplicationForHadoop implements CommandLineRunner {

    @Autowired
    FsShell fsShell;


    public void run(String... strings) throws Exception {
        System.out.println("=========run start============");
        for (FileStatus fileStatus : fsShell.lsr("/")) {
            System.out.println(">" + fileStatus.getPath());
        }
        System.out.println("===========run end===========");

    }

    public static void main(String[] args) {
        System.out.println("===========main start===========");
        SpringApplication.run(SpringBootApplicationForHadoop.class, args);
        System.out.println("===========main end===========");

    }

}
