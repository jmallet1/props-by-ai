
import React from 'react';
import './Home.css';
import SearchBar from './SearchBarHome/SearchBarHome';
import HomeLogo from '../../assets/pictures/home-logo.png';

function Home({ playerList }) {

    const playerNames = [
        { id: 2544, name: 'LeBron James'}
    ]; // List of player names

    return (
        <div className='big'>
            <img className='HomeLogo' src={HomeLogo} alt='HomeLogo'/>
            <SearchBar playerList={playerList} />
        </div>
    );
}

export default Home;