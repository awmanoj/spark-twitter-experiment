import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming._

object TwitterExperiment {
  def main(args: Array[String]) = {
    val checkPointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp")
    val outputDir = sys.env.getOrElse("OUTPUT_DIR","/tmp/tweets")

    TwitterUtils.configureTwitterCredentials()

    // get spark streaming context
    val ssc = new StreamingContext("local[4]", "Twitter Experiment", Seconds(1))

    // new Twitter Stream
    val tweets = ssc.twitterStream()

    val word = if (args.length > 0) args(0) else ""

    println("filtering by word: " + word)

    // process
    val containingHashTags = tweets.filter(_.getText().contains(word))

    val statuses = containingHashTags.map(status => "@" + status.getUser().getScreenName() + ": " + status.getText())
        statuses.print()

    ssc.checkpoint(checkPointDir)

    // start
    ssc.start()
    //ssc.awaitTermination()
  }
}
