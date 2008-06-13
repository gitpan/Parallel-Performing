########################################################################
# Copyright (C) 2008 by Alexander Sviridenko
########################################################################
package Parallel::Performing;

use strict;
use vars qw($VERSION);

$VERSION = '0.01';

use Storable qw(freeze thaw);
use POSIX qw(WNOHANG);
use IO::Select;
use IO::Pipe;

########################################################################
# Constructor
########################################################################

sub new
{
	my $inv = shift;
	my $class = ref($inv) || $inv;
	my $self = {
		'tasks' => {},
		'inactive_tasks' => {},
		'kids' => {},

		'settings' => {
			'kid_max' => 5,
			'timeout' => 5, # sec
			'kid_task' => sub {},
			'kid_result_task' => sub {},
			'kid_timeout_task' => sub {},
			'loop' => sub {},
			@_,	
		},
	
		'is_queue_finished' => 0,
		'is_loop_finished' => 0,
	};
	return bless $self, $class;
}

########################################################################
# Public methods
########################################################################

sub tasks { shift->{'tasks'} }
sub inactive_tasks { shift->{'inactive_tasks'} }
sub kids { shift->{'kids'} }
sub settings { shift->{'settings'} }

sub is_queue_finished { return shift->{'is_queue_finished'} }
sub is_loop_finished { return shift->{'is_loop_finished'} }

sub add_task
{
	my $self = shift;
	my $key = shift;
	$self->tasks->{$key} = [@_];
}

sub remove_task
{
	my $self = shift;
	delete $self->tasks->{+shift};
}

sub get_inactive_task
{
	my $self = shift;
	my $task;

	return $task if ($task) = each %{$self->{'inactive_tasks'}};
	$self->{'inactive_tasks'} = {};
	$self->inactive_tasks->{$_} = 1 for (keys %{$self->{'tasks'}});
	delete @{$self->{'inactive_tasks'}}{ $self->active_tasks };
	return $task if ($task) = each %{$self->{'inactive_tasks'}};

	return undef;
}

sub total_task_count
{
	my $self = shift;
	return scalar keys %{$self->{'tasks'}};
}

sub task_params
{
	my $self = shift;
	my $key = shift;
	return $self->tasks->{$key};
}

sub run_queue
{
	my $self = shift;

	$self->{'is_queue_finished'} = 0;
	$self->{'is_loop_finished'} = 0;

	{
		# reap kids
		while ((my $kid = waitpid(-1,WNOHANG)) > 0)
		{
			delete $self->kids->{$kid};
		}

      	# verify live kids
      	for my $kid (keys %{$self->{'kids'}})
		{
	  		next if kill 0, $kid;
	  		delete $self->kids->{$kid};
		}

      	# launch kids
		if (keys %{$self->{'kids'}} < $self->settings->{'kid_max'}
	  		and my $task = $self->get_inactive_task
	  		and my $kid = $self->create_kid )
	  	{
			$self->send_to_kid($kid, $task);
      	}

		# if any ready results
		READY:
		for my $ready (IO::Select->new(map $_->[1], values %{$self->{'kids'}})->can_read(1))
		{
			my ($kid) = grep $self->kids->{$_}[1] == $ready, keys %{$self->{'kids'}};
	  		{
				last unless read($ready, my $length, 4) == 4;
				$length = unpack 'L', $length;
				last unless read($ready, my $message, $length) == $length;
				$message = thaw($message) or die();
				$self->remove_task($message->[0]);
				$self->settings->{'kid_result_task'}->(@$message);
				my $task;
	    		($task = $self->get_inactive_task) ? $self->send_to_kid($kid, $task) : $self->kids->{$kid}[0]->close;
	    		next READY;
	  		}
	  		# something broken with this kid...
	  		kill 15 => $kid;
	  		delete $self->kids->{$kid}; # forget about it
		}

      	# now timeout kids
      	if (defined $self->settings->{'timeout'})
		{
	  		my $oldest = time - $self->settings->{'timeout'};

			for my $kid (keys %{$self->{'kids'}})
	    	{
	      		next unless defined $self->kids->{$kid}[2];
	      		next unless defined $self->kids->{$kid}[3];
	      		next if $self->kids->{$kid}[3] > $oldest;

	      		if (my $task = $self->kids->{$kid}[2])
				{
		  			my $param_ref = $self->task_params($task);
		  			$self->remove_task($task);
		  			$self->settings->{'kid_timeout_task'}->($task, @$param_ref);
				}

				kill 15 => $kid;
				delete $self->kids->{$kid};
			}
		}

		# is loop finished?
      	unless ($self->settings->{'loop'}->())
		{
			$self->kill_kids;
			$self->{'is_loop_finished'} = 1;
			return;
		}

      	# is queue finished?
		redo if %{$self->{'kids'}} or $self->total_task_count;
		$self->{'is_queue_finished'} = 1;
	}
}

sub kill_kids
{
	my $self = shift;
	foreach (keys %{$self->{'kids'}})
	{
		kill 15 => $_;
	}
}

sub create_kid
{
	my $self = shift;
	my $to_kid = IO::Pipe->new;
	my $from_kid = IO::Pipe->new;
	defined (my $kid = fork) or return;

	unless ($kid)
	{
		$to_kid->reader;
		$from_kid->writer;
		$from_kid->autoflush(1);
		$SIG{$_} = 'DEFAULT' for grep !/^--/, keys %SIG;
		$self->do_kid($to_kid, $from_kid);
		exit(0);
	}

	$from_kid->reader;
	$to_kid->writer;
	$to_kid->autoflush(1);
	$self->kids->{$kid} = [$to_kid, $from_kid];
	$kid;
}

sub do_kid
{
	my $self = shift;
	my ($input, $output) = @_;
	{
		last unless read($input, my $length, 4) == 4;
		$length = unpack 'L', $length;
		last unless read($input, my $message, $length) == $length;
		$message = thaw($message) or die();
		my ($key, @values) = @$message;
		my @results = $self->settings->{'kid_task'}->($key, @values);
		$message = freeze([$key, @results]);
		print $output pack('L', length($message)), $message;
		redo;
	}
	exit(0);
}

sub send_to_kid
{
	my $self = shift;
	my ($kid, $task) = @_;
	{
   		# if SIGPIPE, no biggy, than we'll requeue request later
		local $SIG{PIPE} = 'IGNORE';
      	my $param_ref = $self->task_params($task);
      	my $message = freeze([$task, @$param_ref]);
      	print { $self->kids->{$kid}[0] } pack('L', length($message)), $message;
      	$self->kids->{$kid}[2] = $task;
      	$self->kids->{$kid}[3] = time;
    }
}

sub active_tasks
{
	my $self = shift;
    return grep defined($_), map { $self->kids->{$_}[2] } keys %{$self->{'kids'}};
}

1;
__END__

########################################################################
# POD Documentation
########################################################################

=head1 NAME

Parallel::Performing - Perl extension for the safe performance of the tasks in the queue.

=head1 SYNOPSIS

  use Parallel::Performing;

  $handler = Parallel::Performing->new;

=head1 DESCRIPTION

C<Parallel::Performing> is a module for Perl programs that can safely perform the tasks in the queue.

Features:
* Tasks are performed at the same time in the flow depending on C<kid_max>.
* For each task you can set timeout.
* All tasks consist in the loop. So you can easy exit from queue or kill all tasks, or add new task in the queue without any interrupts.
=head1 METHODS

=head2 Tasks in the queue

=over 4

=item new()

Create a new C<Parallel::Performing>

  my $handler = Parallel::Performing->new( %settings );

where settings are:

=over 4

=item kid_max

Maximum number of kids

  $settings{'kid_max'} = 5; # default

=item timeout

Timeout for single task (in seconds)

  $settings{'timeout'} = 5; # default 

=item kid_task

Argument for this parameter is a function which handles the task and passes the result
to C<kid_result_task>

  $settings{'kid_task'} = sub { };

=item kid_result_task

Argument for this parameter is a function which handles the result. Here you can save it in
ather variables.

  $settings{'kid_result_task'} = sub { };

=item kid_timeout_task

Argument for this parameter is a function which run if was timeout.

  $settings{'kid_timeout_task'} = sub { };

=item loop

Argument for this parameter is a function which returns true(1) if we should continue
and false(0) if necessary to stop. Here you can specify the possible criteria for stopping.   

  $settings{'loop'} = sub { };

=back

=item add_task($key, $task)

Each task should have a key. So when you add a new task, the first parameter should be key and
everything else will be its elements. 

  $handler->add_task($key, $task);

=item run_queue(%settings)

Run the queue

  $handler->run_queue;

=head2 Factor finish

=over 4

=item is_queue_finished()

Return true(1) if queue is finished (all tasks are done) or false(0) 

  if ($handler->is_queue_finished) {
  	
  } else {
  	
  }

=item is_loop_finished()

Return true(1) if loop is finished or false(0)

  if ($handler->is_loop_finished) {
  	
  } else {
  	
  }

=back

=head1 EXAMPLES

This example shows how you can use C<Parallel::Performing> for manipulate connections:

  use Parallel::Performing;
  use LWP::Simple;

  $handler = Parallel::Performing->new;

  %pages = ();
  $handler->add_task('page1', 'http://cpan.org/page1.html');
  $handler->add_task('page2', 'http://cpan.org/page2.html');
  $handler->add_task('page3', 'http://cpan.org/page3.html');

  $handler->run_queue(
    'kid_max' => 3,
    'timeout' => 5, # sec

    'kid_task' => sub {
      ($key, $url) = @_;
      $content = get($url);
      return ($url,$content);
    },

    'kid_result_task' => sub {
      ($key, $url, $content) = @_;
      $pages{$key} = $content;
      return;
    },

    'kid_timeout_task' => sub {
      ($key, $url) = @_;
      print "'$key' is timeouted: ($url)\n";
    },

    'loop' => sub { return 1 },
  );

  
  if ($handler->is_queue_finished) {
    while ( ($page,$content) = each %pages ) {
      print "page: $page\n";
      print "content: $content\n\n";
    }
  }

=back

=head1 SEE ALSO

L<POSIX>, L<Storable>, L<IO::Select>, L<IO::Pipe>

=head1 AUTHOR

Alexander Sviridenko, E<lt>mail@d2rk.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Alexander Sviridenko

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
