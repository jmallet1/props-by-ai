
import React from 'react';
import './Home.css';
import SearchBar from './SearchBarHome/SearchBarHome';
import HomeLogo from '../../assets/pictures/home-logo.png';

function Home() {

  const playerNames = ['Player 1', 'Player 2', 'Player 3']; // List of player names

    return (
        <div className='big'>
            <img className='HomeLogo' src={HomeLogo} alt='HomeLogo'/>
            <SearchBar players={playerNames} />
        </div>
    );
}

export default Home;