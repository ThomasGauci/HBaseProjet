package miage.unice.fr;

public class Triple<T> {
	final T a;
	final T b;
	final T c;

	Triple(final T a, final T b, final T c) {
		this.a = a;
		this.b = b;
		this.c = c;
	}
}