package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@AllArgsConstructor
public class InclusionDependency {

	private static final Set<InclusionDependency> uniqueDependencies = ConcurrentHashMap.newKeySet();

	private final File dependentFile;
	private final String[] dependentAttributes;

	// For example, dependentAttributes from dependentFile are included in referencedAttributes from this particular file referencedFile
	private final File referencedFile;
	private final String[] referencedAttributes;

	/**
	 * Registers this InclusionDependency in the uniqueDependencies set.
	 * @return true if the dependency was added successfully, false if it already exists.
	 */
	public boolean register() {
		return uniqueDependencies.add(this);
	}

	@Override
	public String toString() {
		return this.fileNameOf(this.dependentFile) + " -> " + this.fileNameOf(this.referencedFile) + ": " +
				Arrays.toString(this.dependentAttributes) + " c " + Arrays.toString(this.referencedAttributes);
	}

	private String fileNameOf(File file) {
		return file.getName().split("\\.")[0];
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		InclusionDependency other = (InclusionDependency) o;
		return Objects.equals(this.dependentFile, other.dependentFile) &&
				Arrays.equals(this.dependentAttributes, other.dependentAttributes) &&
				Objects.equals(this.referencedFile, other.referencedFile) &&
				Arrays.equals(this.referencedAttributes, other.referencedAttributes);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(dependentFile, referencedFile);
		result = 31 * result + Arrays.hashCode(dependentAttributes);
		result = 31 * result + Arrays.hashCode(referencedAttributes);
		return result;
	}
}