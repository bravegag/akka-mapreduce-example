package org.akka.essentials.wc.mapreduce.example.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class MapActor extends UntypedActor {

	String[] STOP_WORDS = { "a", "about", "above", "above", "across", "after", "afterwards",
			"again", "against", "all", "almost", "alone", "along", "already", "also", "although",
			"always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
			"any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as",
			"at", "back", "be", "became", "because", "become", "becomes", "becoming", "been",
			"before", "beforehand", "behind", "being", "below", "beside", "besides", "between",
			"beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co",
			"con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down",
			"due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere",
			"empty", "enough", "etc", "even", "ever", "every", "everyone", "everything",
			"everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first",
			"five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full",
			"further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her",
			"here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
			"himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed",
			"interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly",
			"least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
			"mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself",
			"name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody",
			"none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
			"once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
			"ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather",
			"re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she",
			"should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow",
			"someone", "something", "sometime", "sometimes", "somewhere", "still", "such",
			"system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then",
			"thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon",
			"these", "they", "thickv", "thin", "third", "this", "those", "though", "three",
			"through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward",
			"towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
			"very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence",
			"whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon",
			"wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom",
			"whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your",
			"yours", "yourself", "yourselves", "the" };

	List<String> STOP_WORDS_LIST = Arrays.asList(STOP_WORDS);

	private ActorRef actor = null;

	public MapActor(ActorRef inReduceActor) {
		actor = inReduceActor;
	}

	private List<Result> evaluateExpression(String line) {
		List<Result> list = new ArrayList<Result>();
		StringTokenizer parser = new StringTokenizer(line);
		while (parser.hasMoreTokens()) {
			String word = parser.nextToken().toLowerCase();
			if (isAlpha(word) && !STOP_WORDS_LIST.contains(word)) {
				list.add(new Result(word, 1));
			}
		}
		return list;
	}

	private boolean isAlpha(String s) {
		s = s.toUpperCase();
		for (int i = 0; i < s.length(); i++) {
			int c = (int) s.charAt(i);
			if (c < 65 || c > 90)
				return false;
		}
		return true;
	}

	// message handler
	public void onReceive(Object message) {
		System.out.println("MapActor -> onReceive(" + message + ")");
		if (message instanceof String) {
			String work = (String) message;
			if ("Thieves! thieves!".equals(work)) {
				try {
					System.out.println("*** sleeping!");
					Thread.sleep(5000);
					System.out.println("*** back!");
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// perform the work
			List<Result> list = evaluateExpression(work);

			// reply with the result
			actor.tell(list, getSelf());

		}
		else
			throw new IllegalArgumentException("Unknown message [" + message + "]");
	}
}
