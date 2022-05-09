# Init renv and ensure that Eunomia/Strategus are excluded in the lock file
renv::init(settings = list(ignored.packages = c("Eunomia", "Strategus")))