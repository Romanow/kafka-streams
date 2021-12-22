package ru.romanow.stream.processor;

import org.junit.jupiter.params.provider.Arguments;

import java.util.Set;
import java.util.stream.Stream;

abstract class StreamProcessorCommonTest {

    static Stream<Arguments> factory() {
        return Stream.of(
                Arguments.of("data/Cats and Dogs.txt", Set.of("cats", "dogs", "big", "kittens")),
                Arguments.of("data/Harry Potter and Philosopher Stone – Sorting Hat song.txt", Set.of("oh", "you", "may", "not", "think", "i'm", "pretty", "but", "don't", "judge", "on", "what", "see", "i'll", "eat", "myself", "if", "can", "find", "a", "smarter", "hat", "than", "me", "keep", "your", "bowlers", "black", "top", "hats", "sleek", "and", "tall", "for", "the", "hogwarts", "sorting", "i", "cap", "them", "all", "there's", "nothing", "hidden", "in", "head", "can't", "so", "try", "will", "tell", "where", "ought", "to", "be", "might", "belong", "gryffindor", "dwell", "brave", "at", "heart", "their", "daring", "nerve", "chivalry", "set", "gryffindors", "apart", "hufflepuff", "they", "are", "just", "loyal", "those", "patient", "hufflepuffs", "true", "unafraid", "of", "toil", "or", "yet", "wise", "old", "ravenclaw", "you've", "ready", "mind", "wit", "learning", "always", "kind", "perhaps", "slytherin", "you'll", "make", "real", "friends", "cunning", "folks", "use", "any", "means", "achieve", "ends", "put", "afraid", "get", "flap", "you're", "safe", "hands", "though", "have", "none", "thinking")),
                Arguments.of("data/Harry Potter and Goblet of File – Sphinx's poem.txt", Set.of("first", "think", "of", "the", "person", "who", "lives", "in", "disguise", "deals", "secrets", "and", "tells", "naught", "but", "lies", "next", "tell", "me", "what’s", "always", "last", "thing", "to", "mend", "middle", "end", "finally", "give", "sound", "often", "heard", "during", "search", "for", "a", "hard-to-find", "word", "now", "string", "them", "together", "answer", "this", "which", "creature", "would", "you", "be", "unwilling", "kiss")),
                Arguments.of("data/Dr. Seuss – Green Eggs & Ham.txt", Set.of("i", "do", "not", "like", "them", "in", "a", "box", "with", "fox", "house", "mouse", "here", "or", "there", "anywhere", "green", "eggs", "and", "ham", "sam", "am")),
                Arguments.of("data/Edgar Allan Poe – A Dream Within A Dream.txt", Set.of("take", "this", "kiss", "upon", "the", "brow", "and", "in", "parting", "from", "you", "now", "thus", "much", "let", "me", "avow", "are", "not", "wrong", "who", "deem", "that", "my", "days", "have", "been", "a", "dream", "yet", "if", "hope", "has", "flown", "away", "night", "or", "day", "vision", "none", "is", "it", "therefore", "less", "gone", "all", "we", "see", "seem", "but", "within", "i", "stand", "amid", "roar", "of", "surf-tormented", "shore", "hold", "hand", "grains", "golden", "sand", "how", "few", "they", "creep", "through", "fingers", "to", "deep", "while", "weep", "o", "god", "can", "grasp", "them", "with", "tighter", "clasp", "save", "one", "pitiless", "wave")),
                Arguments.of("data/Joyce Kilmer – Trees.txt", Set.of("i", "think", "that", "shall", "never", "see", "a", "poem", "lovely", "as", "tree", "whose", "hungry", "mouth", "is", "prest", "against", "the", "earth’s", "sweet", "flowing", "breast", "looks", "at", "god", "all", "day", "and", "lifts", "her", "leafy", "arms", "to", "pray", "may", "in", "summer", "wear", "nest", "of", "robins", "hair", "upon", "bosom", "snow", "has", "lain", "who", "intimately", "lives", "with", "rain", "poems", "are", "made", "by", "fools", "like", "me", "but", "only", "can", "make")),
                Arguments.of("data/Robert Frost – Fire & Ice.txt", Set.of("some", "say", "the", "world", "will", "end", "in", "fire", "ice", "from", "what", "i’ve", "tasted", "of", "desire", "i", "hold", "with", "those", "who", "favour", "but", "if", "it", "had", "to", "perish", "twice", "think", "know", "enough", "hate", "that", "for", "destruction", "is", "also", "great", "and", "would", "suffice"))
        );
    }
}
