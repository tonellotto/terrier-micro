package it.cnr.isti.hpclab.annotations;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention; 

@Retention(RUNTIME)
public @interface Managed 
{
	public String by(); 
}
