DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `user_id` varchar(19) NOT NULL,
  `password` text NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

DROP TABLE IF EXISTS `userinfo`;
CREATE TABLE `userinfo` (
  `user_id` varchar(19) NOT NULL,
  `username` text NOT NULL,
  `profile` text NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

