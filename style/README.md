### ATTENTION: files in this directory are automatically copied from git@github.com:ddf-project/DDF/tree/master/style. If you have modifications, make them there. Modifications made here will be overwritten.

Generated: Sun Jun 30 18:24:10 PDT 2013 by ctn@ctnu

### Scala

Import `Scala-Formatter.xml` via Eclipse Preferences > Scala > Formatter > Import.

Important for MacOS/Windows users: you must change the default (or at least project) encoding from MacRoman/Cp1252 to UTF-8. Do this via Eclipse Preferences > General > Workspace > Text file encoding.

### Java

Import `Java-CodeStyle-Formatter.xml` via Eclipse Preferences > Java > Code Style > Formatter > Import.

Currently, we decide to use **Eclipse [built-in]** code convention, which is the default setting of Eclipse. If you do not use Eclipse, please take a look at Java-CodeStyle-Formatter.xml in this directory and import to your IDE if possible.

Some Eclipse useful shortcuts:

* Ctrl+Shift+F: Automatically format the source code to the default setting
* Ctrl+Shift+O: Automatically import necessary classes and remove unnecessary ones
* Ctrl+Shift+L: Display other shortcuts

Any later changes will be noticed here.

ctn Sun Jun 30 17:27:39 PDT 2013
* Added newlines before else, catch, finally, etc. to enhance readability, while not introducing too many whitespaces.

### R
R code is much more smaller and shorter than Java code. So we can write code, adopt coding styles incrementally to accumulate our own R style. In addition, RStudio is a great IDE which helps much on R coding style. If you copy & paste the R code into RStudio, the IDE will automatically reindent lines, also you can select the code and choose menu Code => Reindent Lines to do it manually

Sources for R Style:

* http://google-styleguide.googlecode.com/svn/trunk/google-r-style.html
* https://github.com/hadley/devtools/wiki/Style

### Code templates

Code templates are useful for oft-used snippets in Eclipse. You can import them via Eclipse Preference > Java > Code Style > Code Templates for Java, and Eclipse Preference > Scala > Code Templates for Scala. We have:

    Java-CodeStyle->CodeTemplates.xml
      timerstart
      timerstopreport

    Scala-Templates.xml (not yet available)
